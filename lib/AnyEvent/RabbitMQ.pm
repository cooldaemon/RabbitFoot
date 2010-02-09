package AnyEvent::RabbitMQ;

use Data::Dumper;
use List::MoreUtils qw(none);

use AnyEvent::Handle;
use AnyEvent::Socket;

use Net::AMQP;
use Net::AMQP::Common qw(:all);

use AnyEvent::RabbitMQ::Channel;
use AnyEvent::RabbitMQ::LocalQueue;

use Moose;
use MooseX::AttributeHelpers;

our $VERSION = '0.01';

has verbose => (
    isa => 'Bool',
    is  => 'rw',
);

has timeout => (
    isa     => 'Int',
    is      => 'rw',
    default => 1,
);

has _connect_guard => (
    isa     => 'Guard',
    is      => 'ro',
    clearer => 'clear_connect_guard',
);

has _handle => (
    isa     => 'AnyEvent::Handle',
    is      => 'ro',
    clearer => 'clear_handle',
);

has _is_open => (
    isa     => 'Bool',
    is      => 'ro',
    default => 0,
);

has channels => (
    metaclass => 'Collection::Hash',
    is        => 'ro',
    isa       => 'HashRef[AnyEvent::RabbitMQ::Channel]',
    default   => sub {{}},
    provides  => {
        set    => 'set_channel',
        get    => 'get_channel',
        empty  => 'has_channels',
        delete => 'delete_channel',
        keys   => 'channel_ids',
        count  => 'count_channels',
    },
);

has _queue => (
    isa     => 'AnyEvent::RabbitMQ::LocalQueue',
    is      => 'ro',
    default => sub {
        AnyEvent::RabbitMQ::LocalQueue->new
    },
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub load_xml_spec {
    my ($self, $file,) = @_;
    Net::AMQP::Protocol->load_xml_spec($file); # die when fail in this line.
    return $self;
}

sub connect {
    my $self = shift;
    my %args = $self->_set_cbs(@_);

    if ($self->verbose) {
        print STDERR 'connect to ', $args{host}, ':', $args{port}, '...', "\n";
    }

    $self->{_connect_guard} = AnyEvent::Socket::tcp_connect(
        $args{host},
        $args{port},
        sub {
            my $fh = shift
                or return $args{on_failure}->('Error connecting to AMQP Server!');
            $self->{_handle} = AnyEvent::Handle->new(
                fh       => $fh,
                on_error => sub {
                    my ($handle, $fatal, $message) = @_;
                    $self->clear_handle;
                    $args{on_failure}->($message);
                }
            );
            $self->_read_loop($args{on_failure});
            $self->_start(%args,);
        },
        sub {
            return $self->timeout;
        },
    );

    return $self;
}

sub _read_loop {
    my ($self, $failure_cb,) = @_;

    return if !$self->_handle;

    $self->_handle->push_read(chunk => 8, sub {
        my $data = $_[1];
        my $stack = $_[1];

        if (length($data) <= 0) {
            $failure_cb->('Disconnect');
            @_ = ($self, $failure_cb,);
            goto &_read_loop;
        }

        my ($type_id, $channel, $length,) = unpack 'CnN', substr $data, 0, 7, '';
        if (!defined $type_id || !defined $channel || !defined $length) {
            $failure_cb->('Broken data was received');
            @_ = ($self, $failure_cb,);
            goto &_read_loop;
        }

        $self->_handle->push_read(chunk => $length, sub {
            $stack .= $_[1];
            my ($frame) = Net::AMQP->parse_raw_frames(\$stack);

            if ($self->verbose) {
                print STDERR '[C] <-- [S] ' . Dumper($frame);
                print STDERR '-----------', "\n";
            }

            return if !$self->_check_close_and_clean($frame, $failure_cb,);

            my $id = $frame->channel;
            if (0 == $id) {
                $self->_queue->push($frame);
            } elsif ($self->has_channels($id)) {
                $self->get_channel($id)->_push_queue_or_consume($frame, $failure_cb);
            } else {
                $failure_cb->('Unknown channel id: ' . $frame->channel);
            }

            @_ = ($self, $failure_cb,);
            goto &_read_loop;
        });
    });

    return $self;
}

sub _check_close_and_clean {
    my ($self, $frame, $failure_cb, $id,) = @_;

    return 1 if !$frame->isa('Net::AMQP::Frame::Method');

    my $method_frame = $frame->method_frame;

    if ($method_frame->isa('Net::AMQP::Protocol::Connection::Close')) {
        $self->_push_write(Net::AMQP::Protocol::Connection::CloseOk->new());
        $self->{_is_open} = 0;
        $self->_disconnect();
        $failure_cb->(
            $method_frame->reply_code . ' ' . $method_frame->reply_text
        );
        return;
    } elsif ($method_frame->isa('Net::AMQP::Protocol::Channel::Close')) {
        $self->_push_write(Net::AMQP::Protocol::Channel::CloseOk->new(), $id,);

        my $id = $frame->channel;
        my $message = $method_frame->reply_code . ' ' . $method_frame->reply_text;

        my $channel = $self->get_channel($id);
        if (!$channel) {
            $failure_cb->("Unknown channel id: ${id}\n${message}");
            return;
        }

        $channel->_is_open(0);
        $self->delete_channel($id);
        $failure_cb->($message);
        return;
    }

    return 1;
}

sub _start {
    my $self = shift;
    my %args = @_;

    if ($self->verbose) {
        print STDERR 'post header', "\n";
    }

    $self->_handle->push_write(Net::AMQP::Protocol->header);

    $self->_push_read_and_valid(
        'Connection::Start',
        sub {
            my $frame = shift;

            my @mechanisms = split /\s/, $frame->method_frame->mechanisms;
            return $args{on_failure}->('AMQPLAIN is not found in mechanisms')
                if none {$_ eq 'AMQPLAIN'} @mechanisms;

            my @locales = split /\s/, $frame->method_frame->locales;
            return $args{on_failure}->('en_US is not found in locales')
                if none {$_ eq 'en_US'} @locales;

            $self->_push_write(
                Net::AMQP::Protocol::Connection::StartOk->new(
                    client_properties => {
                        platform    => 'Perl',
                        product     => __PACKAGE__,
                        information => 'http://d.hatena.ne.jp/cooldaemon/',
                        version     => '0.01',
                    },
                    mechanism => 'AMQPLAIN',
                    response => {
                        LOGIN    => $args{user},
                        PASSWORD => $args{pass},
                    },
                    locale => 'en_US',
                ),
            );

            $self->_tune(%args,);
        },
        $args{on_failure},
    );

    return $self;
}

sub _tune {
    my $self = shift;
    my %args = @_;

    $self->_push_read_and_valid(
        'Connection::Tune',
        sub {
            my $frame = shift;

            $self->_push_write(
                Net::AMQP::Protocol::Connection::TuneOk->new(
                    channel_max => $frame->method_frame->channel_max,
                    frame_max   => $frame->method_frame->frame_max,
                    heartbeat   => $frame->method_frame->heartbeat,
                ),
            );

            $self->_open(%args,);
        },
        $args{on_failure},
    );

    return $self;
}

sub _open {
    my $self = shift;
    my %args = @_;

    $self->_push_write_and_read(
        'Connection::Open',
        {
            virtual_host => $args{vhost},
            capabilities => '',
            insist       => 1,
        },
        'Connection::OpenOk', 
        sub {
            $self->{_is_open} = 1;
            $args{on_success}->($self);
        },
        $args{on_failure},
    );

    return $self;
}

sub close {
    my $self = shift;
    my %args = $self->_set_cbs(@_);

    my $close_cb = sub {
        $self->_close(
            sub {
                $self->_disconnect();
                $args{on_success}->(@_);
            },
            sub {
                $self->_disconnect();
                $args{on_failure}->(@_);
            }
        );
        return $self;
    };

#   if (0 == $self->count_channels) {
    if (0 == scalar keys %{$self->channels}) { # FIXME
        return $close_cb->();
    }

#    for my $id ($self->channel_ids) {
    for my $id (keys %{$self->channels}) { # FIXME
#        $self->get_channel($id)->close(
         $self->channels->{$id}->close( # FIXME
            on_success => $close_cb,
            on_failure => sub {
                $close_cb->();
                $args{on_failure}->(@_);
            },
        );
    }

    return $self;
}

sub _close {
    my ($self, $cb, $failure_cb,) = @_;

    return $self if !$self->_is_open || 0 < $self->count_channels;

    $self->_push_write_and_read(
        'Connection::Close', {}, 'Connection::CloseOk',
        $cb, $failure_cb,
    );
    $self->{_is_open} = 0;

    return $self;
}

sub _disconnect {
    my ($self,) = @_;

    $self->clear_connect_guard;
    $self->clear_handle;

    return;
}

sub open_channel {
    my $self = shift;
    my %args = $self->_set_cbs(@_);

    my $id = $args{id};
    return $args{on_failure}->("Channel id $id is already in use")
        if $id && $self->has_channels($id);

    if (!$id) {
        for my $candidate_id (1 .. (2**16 - 1)) { # FIXME
            next if $self->has_channels($candidate_id);
            $id = $candidate_id;
            last;
        }
        return $args{on_failure}->('Ran out of channel ids') if !$id;
    }

    my $channel = AnyEvent::RabbitMQ::Channel->new(
        id         => $id,
        connection => $self,
    );

    $self->set_channel($id => $channel);

    $channel->open(
        on_success => sub {
            $args{on_success}->($channel);
        },
        on_failure => sub {
            $self->delete_channel($id);
            $args{on_failure}->(@_);
        },
    );

    return $self;
}

sub _push_write_and_read {
    my ($self, $method, $args, $exp, $cb, $failure_cb, $id,) = @_;

    $method = 'Net::AMQP::Protocol::' . $method;
    $self->_push_write(
        Net::AMQP::Frame::Method->new(
            method_frame => $method->new(%$args)
        ),
        $id,
    );

    return $self->_push_read_and_valid($exp, $cb, $failure_cb, $id,);
}

sub _push_read_and_valid {
    my ($self, $exp, $cb, $failure_cb, $id,) = @_;
    $exp = ref($exp) eq 'ARRAY' ? $exp : [$exp];

    my $queue = $id ? $self->get_channel($id)->_queue : $self->_queue;

    $queue->get(sub {
        my $frame = shift;

        return $failure_cb->('Received data is not method frame')
            if !$frame->isa('Net::AMQP::Frame::Method');

        my $method_frame = $frame->method_frame;
        for my $exp_elem (@$exp) {
            return $cb->($frame)
                if $method_frame->isa('Net::AMQP::Protocol::' . $exp_elem);
        }

        $failure_cb->(
              'Method is not ' . join(',', @$exp) . "\n"
            . 'Method was ' . ref $method_frame
        );
    });
}

sub _push_read {
    my ($self, $cb, $failure_cb) = @_;

    $self->_handle->push_read(chunk => 8, sub {
        my $data = $_[1];
        my $stack = $_[1];
        return $failure_cb->('Disconnect') if length($data) <= 0;

        my ($type_id, $channel, $length,) = unpack 'CnN', substr $data, 0, 7, '';
        return $failure_cb->('Broken data was received')
            if !defined $type_id || !defined $channel || !defined $length;

        $self->_handle->push_read(chunk => $length, sub {
            $stack .= $_[1];
            my ($frame) = Net::AMQP->parse_raw_frames(\$stack);

            if ($self->verbose) {
                print STDERR '[C] <-- [S] ' . Dumper($frame);
                print STDERR '-----------', "\n";
            }

            $cb->($frame);
        });
    });
}

sub _push_write {
    my ($self, $output, $id,) = @_;

    if ($output->isa('Net::AMQP::Protocol::Base')) {
        $output = $output->frame_wrap;
    }
    $output->channel($id || 0);

    if ($self->verbose) {
        print STDERR '[C] --> [S] ', Dumper($output), "\n";
    }

    $self->_handle->push_write($output->to_raw_frame());
    return;
}

sub _set_cbs {
    my $self = shift;
    my %args = @_;

    $args{on_success} ||= sub {};
    $args{on_failure} ||= sub {die @_};

    return %args;
}

sub DEMOLISH {
    my ($self) = @_;

    $self->close();
    return;
}

1;
__END__

=head1 NAME

AnyEvent::RabbitMQ - An asynchronous and multi channel Perl AMQP client.

=head1 SYNOPSIS

  use AnyEvent::RabbitMQ;

  my $cv = AnyEvent->condvar;

  my $ar = AnyEvent::RabbitMQ->new(
      timeout => 1,
  )->load_xml_spec(
      '/path/to/amqp0-8.xml',
  )->connect(
      host       => 'localhosti',
      port       => 5672,
      user       => 'guest',
      port       => 'guest',
      vhost      => '/',
      on_success => sub {
          $ar->open_channel(
              on_success => sub {
                  my $channel = shift;
                  $channel->declare_exchange(
                      exchange   => 'test_exchange',
                      on_success => sub {
                          $cv->send('Declared exchange');
                      },
                      on_failure => $cv,
                  );
              },
              on_failure => $cv,
          );
      },
      on_failure => $cv,
  );

  print $cv->recv, "\n";

=head1 DESCRIPTION

AnyEvent::RabbitMQ is an AMQP(Advanced Message Queuing Protocol) client library, that is intended to allow you to interact with AMQP-compliant message brokers/servers such as RabbitMQ in a Asynchronous fashion.

You can use AnyEvent::RabbitMQ to -

  * Declare and delete exchanges
  * Declare, delete, bind and unbind queues
  * Set QoS
  * Publish, consume, get, ack and recover messages
  * Select, commit and rollback transactions

AnyEvnet::RabbitMQ is known to work with RabbitMQ versions 1.7.1 and version 0-8 of the AMQP specification.

=head1 AUTHOR

Masahito Ikuta E<lt>cooldaemon@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
