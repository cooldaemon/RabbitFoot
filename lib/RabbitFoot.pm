package RabbitFoot;

use Data::Dumper;
use List::MoreUtils qw(none);
use IO::Socket::INET;
use Sys::SigAction qw(timeout_call);
use Net::AMQP;
use Net::AMQP::Common qw(:all);

use Moose;

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

has publish_timeout => (
    isa     => 'Int',
    is      => 'rw',
    default => 1,
);

has _socket => (
    isa     => 'IO::Socket::INET',
    is      => 'rw',
    clearer => 'clear_socket',
);

has _is_open => (
    isa     => 'Bool',
    is      => 'rw',
    default => 0,
);

has _is_oepn_channel => (
    isa     => 'Bool',
    is      => 'rw',
    default => 0,
);

has _consume_tag => (
    isa     => 'Str',
    is      => 'rw',
    default => '',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub load_xml_spec {
    my ($self, $file,) = @_;

    eval {
        Net::AMQP::Protocol->load_xml_spec($file);
    };
    die $@, "\n" if $@;

    return $self;
}

sub connect {
    my ($self, $args) = @_;

    eval {
        $self->_connect(
            $args,
        )->_start(
            $args,
        )->_tune(
        )->_open(
            $args,
        )->_open_channel(
        );
    };

    return $self if !$@;

    my $exception = $@;
    $self->close();
    die $exception;
}

sub _connect {
    my ($self, $args,) = @_;

    if ($self->verbose) {
        print STDERR 'connect to ', $args->{host}, ':', $args->{port}, '...', "\n";
    }

    my $socket = IO::Socket::INET->new(
        Proto    => 'tcp',
        PeerAddr => $args->{host},
        PeerPort => $args->{port},
        Timeout  => $self->timeout,
    ) or die 'Error connecting to AMQP Server!', "\n";

    $self->_socket($socket);
    return $self;
}

sub _start {
    my ($self, $args,) = @_;

    if ($self->verbose) {
        print STDERR 'post header', "\n";
    }

    print {$self->_socket} Net::AMQP::Protocol->header;

    my $frame = $self->_read_and_valid('Connection::Start');

    my @mechanisms = split /\s/, $frame->method_frame->mechanisms;
    die 'AMQPLAIN is not found in mechanisms', "\n"
        if none {$_ eq 'AMQPLAIN'} @mechanisms;

    my @locales = split /\s/, $frame->method_frame->locales;
    die 'en_US is not found in locales', "\n"
        if none {$_ eq 'en_US'} @locales;

    $self->_post(
        Net::AMQP::Protocol::Connection::StartOk->new(
            client_properties => {
                platform    => 'Perl',
                product     => __PACKAGE__,
                information => 'http://d.hatena.ne.jp/cooldaemon/',
                version     => '0.01',
            },
            mechanism => 'AMQPLAIN',
            response => {
                LOGIN    => $args->{user},
                PASSWORD => $args->{pass},
            },
            locale => 'en_US',
        ),
    );

    return $self;
}

sub _tune {
    my ($self,) = @_;

    my $frame = $self->_read_and_valid('Connection::Tune');

    $self->_post(
        Net::AMQP::Protocol::Connection::TuneOk->new(
            channel_max => $frame->method_frame->channel_max,
            frame_max   => $frame->method_frame->frame_max,
            heartbeat   => $frame->method_frame->heartbeat,
        ),
    );

    return $self;
}

sub _open {
    my ($self, $args,) = @_;

    $self->_post_and_read(
        'Connection::Open',
        {
            virtual_host => $args->{vhost},
            capabilities => '',
            insist       => 1,
        },
        'Connection::OpenOk', 
    );
    $self->_is_open(1);

    return $self;
}

sub close {
    my ($self,) = @_;

    for my $method (qw(cancel _close_channel _close _disconnect)) {
        eval {$self->$method()};
    }

    return $self;
}

sub _close {
    my ($self,) = @_;

    return $self if !$self->_is_open;

    $self->_post_and_read('Connection::Close', {}, 'Connection::CloseOk',);
    $self->_is_open(0);

    return $self;
}

sub _disconnect {
    my ($self,) = @_;

    return if !$self->_socket;

    CORE::close $self->_socket;
    $self->clear_socket;

    return;
}

sub _open_channel {
    my ($self) = @_;

    $self->_post_and_read('Channel::Open', {}, 'Channel::OpenOk', 1,);
    $self->_is_oepn_channel(1);

    return $self;
}

sub _close_channel {
    my ($self,) = @_;

    return $self if !$self->_is_oepn_channel;

    $self->_post_and_read('Channel::Close', {}, 'Channel::CloseOk', 1,);
    $self->_is_oepn_channel(0);

    return $self;
}

sub declare_exchange {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Exchange::Declare',
        {
            type        => 'direct',
            passive     => 0,
            durable     => 0,
            auto_delete => 0,
            internal    => 0,
            %$args, # exchange
            ticket      => 0,
            nowait      => 0,
        },
        'Exchange::DeclareOk', 
        1,
    );
}

sub delete_exchange {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Exchange::Delete',
        {
            if_unused => 0,
            %$args, # exchange
            ticket    => 0,
            nowait    => 0,
        },
        'Exchange::DeleteOk', 
        1,
    );
}

sub declare_queue {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Queue::Declare',
        {
            queue       => '',
            passive     => 0,
            durable     => 0,
            exclusive   => 0,
            auto_delete => 0,
            %$args,
            ticket      => 0,
            no_ack      => 1,
            nowait      => 0,
        },
        'Queue::DeclareOk', 
        1,
    );
}

sub bind_queue {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Queue::Bind',
        {
            %$args, # queue, exchange, routing_key
            ticket      => 0,
            nowait      => 0,
        },
        'Queue::BindOk', 
        1,
    );
}

sub purge_queue {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Queue::Purge',
        {
            %$args, # queue
            ticket => 0,
            nowait => 0,
        },
        'Queue::PurgeOk', 
        1,
    );
}

sub delete_queue {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Queue::Delete',
        {
            if_unused => 0,
            if_empty  => 0,
            %$args, # queue
            ticket    => 0,
            nowait    => 0,
        },
        'Queue::DeleteOk', 
        1,
    );
}

sub publish {
    my ($self, $publish_args, $header_args, $message,) = @_;

    $self->_publish(
        $publish_args,
    )->_header(
        $header_args, $message,
    )->_body(
        $message,
    );

    return if !$publish_args->{mandatory} && !$publish_args->{immediate};

    my $frame = eval {
        $self->_read_and_valid('Basic::Return', $self->publish_timeout);
    };

    if ($@) {
        return if $@ =~ '^Read\stimed\sout';
        die $@;
    }

    return {
        return => $frame,
        header => $self->_read_header_and_valid(),
        body   => $self->_read_body_and_valid(),
    };
}

sub _publish {
    my ($self, $args,) = @_;

    $self->_post(
        Net::AMQP::Protocol::Basic::Publish->new(
            exchange  => '',
            mandatory => 0,
            immediate => 0,
            %$args, # routing_key
            ticket    => 0,
        ),
        1,
    );

    return $self;
}

sub _header {
    my ($self, $args, $message,) = @_;

    $self->_post(
        Net::AMQP::Frame::Header->new(
            weight       => $args->{weight} || 0,
            body_size    => length($message),
            header_frame => Net::AMQP::Protocol::Basic::ContentHeader->new(
                content_type     => 'application/octet-stream',
                content_encoding => '',
                headers          => {},
                delivery_mode    => 1,
                priority         => 1,
                correlation_id   => '',
                reply_to         => '',
                expiration       => '',
                message_id       => '',
                timestamp        => time,
                type             => '',
                user_id          => '',
                app_id           => '',
                cluster_id       => '',
                %$args,
            ),
        ),
        1,
    );

    return $self;
}

sub _body {
    my ($self, $message,) = @_;
    $self->_post(Net::AMQP::Frame::Body->new(payload => $message), 1);
    return $self;
}

sub consume {
    my ($self, $args,) = @_;

    die 'Has already been consuming message', "\n" if $self->_consume_tag;

    my $frame = $self->_post_and_read(
        'Basic::Consume',
        {
            consumer_tag => '',
            no_local     => 0,
            no_ack       => 1,
            exclusive    => 0,
            %$args, # queue
            ticket       => 0,
            nowait       => 0,
        },
        'Basic::ConsumeOk', 
        1,
    );

    $self->_consume_tag($frame->method_frame->consumer_tag);
    return $frame;
}

sub cancel {
    my ($self,) = @_;

    return if !$self->_consume_tag;

    my $frame = $self->_post_and_read(
        'Basic::Cancel',
        {
            consumer_tag => $self->_consume_tag,
            nowait       => 0,
        },
        'Basic::CancelOk', 
        1,
    );

    $self->_consume_tag('');
    return $frame;
}

sub poll {
    my ($self, $args,) = @_;

    my $timeout = $args && $args->{timeout} ? $args->{timeout} : 'infinite';
    return {
        deliver => $self->_read_and_valid('Basic::Deliver', $timeout),
        header  => $self->_read_header_and_valid(),
        body    => $self->_read_body_and_valid(),
    };
}

sub get {
    my ($self, $args,) = @_;

    my $frame = $self->_post_and_read(
        'Basic::Get',
        {
            no_ack => 1,
            %$args, # queue
            ticket => 0,
        },
        [qw(Basic::GetOk Basic::GetEmpty)], 
        1,
    );

    return $frame
        if $frame->method_frame->isa('Net::AMQP::Protocol::Basic::GetEmpty');

    return {
        getok  => $frame,
        header => $self->_read_header_and_valid(),
        body   => $self->_read_body_and_valid(),
    };
}

sub ack {
    my ($self, $args,) = @_;

    $self->_post(
        Net::AMQP::Protocol::Basic::Ack->new(
            delivery_tag => 0,
            multiple     => (
                defined $args->{delivery_tag} && $args->{delivery_tag} != 0 ? 0 : 1
            ),
            %$args,
        ),
        1,
    );
    return;
}

sub qos {
    my ($self, $args,) = @_;

    return $self->_post_and_read(
        'Basic::Qos',
        {
            prefetch_count => 1,
            %$args,
            prefetch_size  => 0,
            global         => 0,
        },
        'Basic::QosOk', 
        1,
    );
}

sub recover {
    my ($self, $args,) = @_;

    $self->_post(
        Net::AMQP::Protocol::Basic::Recover->new(
            requeue => 0,
            %$args,
        ),
        1,
    );
    return;
}

sub select_tx {
    my ($self,) = @_;
    return $self->_post_and_read('Tx::Select', {}, 'Tx::SelectOk', 1,);
}

sub commit_tx {
    my ($self,) = @_;
    return $self->_post_and_read('Tx::Commit', {}, 'Tx::CommitOk', 1,);
}

sub rollback_tx {
    my ($self,) = @_;
    return $self->_post_and_read('Tx::Rollback', {}, 'Tx::RollbackOk', 1,);
}

sub _post_and_read {
    my ($self, $method, $args, $exp, $id,) = @_;

    $method = 'Net::AMQP::Protocol::' . $method;
    $self->_post(
        Net::AMQP::Frame::Method->new(
            method_frame => $method->new(%$args)
        ),
        $id,
    );
    return $self->_read_and_valid($exp);
}

sub _read_and_valid {
    my ($self, $exp, $timeout,) = @_;
    $exp = ref($exp) eq 'ARRAY' ? $exp : [$exp];

    my $frame = $self->_read($timeout);
    die 'Received data is not method frame', "\n"
        if !$frame->isa('Net::AMQP::Frame::Method');

    my $method_frame = $frame->method_frame;
    for my $exp_elem (@$exp) {
        return $frame if $method_frame->isa('Net::AMQP::Protocol::' . $exp_elem);
    }

    $self->_check_close_and_clean($frame);
    die 'Method is not ', join(',', @$exp), "\n",
        'Method was ', ref $method_frame, "\n"
            if !$method_frame->isa('Net::AMQP::Protocol::Connection::Close');
}

sub _read_header_and_valid {
    my ($self,) = @_;

    my $frame = $self->_read();
    if (!$frame->isa('Net::AMQP::Frame::Header')) {
        $self->_check_close_and_cleanup($frame);
        die 'Received data is not header frame', "\n";
    }

    my $header_frame = $frame->header_frame;
    die 'Header is not Protocol::Basic::ContentHeader',
        'Header was ', ref $header_frame, "\n"
            if !$header_frame->isa('Net::AMQP::Protocol::Basic::ContentHeader');

    return $frame;
}

sub _read_body_and_valid {
    my ($self,) = @_;

    my $frame = $self->_read();
    return $frame if $frame->isa('Net::AMQP::Frame::Body');

    $self->_check_close_and_cleanup($frame);
    die 'Received data is not body frame', "\n";
}

sub _check_close_and_clean {
    my ($self, $frame,) = @_;

    return $self if !$frame->isa('Net::AMQP::Frame::Method');

    my $method_frame = $frame->method_frame;

    if ($method_frame->isa('Net::AMQP::Protocol::Connection::Close')) {
        $self->_is_oepn_channel(0);
        $self->_post(Net::AMQP::Protocol::Connection::CloseOk->new());
        $self->_is_open(0);
        $self->_disconnect();
        die $method_frame->reply_code, ' ', $method_frame->reply_text, "\n";
    } elsif ($method_frame->isa('Net::AMQP::Protocol::Channel::Close')) {
        $self->_post(Net::AMQP::Protocol::Channel::CloseOk->new(), 1);
        $self->_is_oepn_channel(0);
        $self->_close()->_disconnect();
        die $method_frame->reply_code, ' ', $method_frame->reply_text, "\n";
    }

    return $self;
}

sub _read {
    my ($self, $timeout,) = @_;

    $timeout ||= $self->timeout;

    my $frame;

    if ($timeout eq 'infinite') {
        $frame = $self->_do_read();
    } else {
        if (timeout_call($timeout, sub {$frame = $self->_do_read()})) {
            die 'Read timed out after', $timeout, "\n";
        }
    }

    return $frame;
}

sub _do_read {
    my ($self,) = @_;

    my $stack;
    my $data;

    read $self->_socket, $data, 8;
    if (length($data) <= 0) {
        die 'Disconnect', "\n";
    }

    $stack .= $data;

    my ($type_id, $channel, $length,) = unpack 'CnN', substr $data, 0, 7, '';
    if (!defined $type_id || !defined $channel || !defined $length) {
        die 'Broken data was received', "\n";
    }

    while ($length > 0) {
        $length -= read $self->_socket, $data, $length;
        $stack .= $data;
    }

    my ($frame) = Net::AMQP->parse_raw_frames(\$stack);
    if ($self->verbose) {
        print STDERR '[C] <-- [S] ' . Dumper($frame);
        print STDERR '-----------', "\n";
    }

    return $frame;
}

sub _post {
    my ($self, $output, $id,) = @_;

    if ($output->isa('Net::AMQP::Protocol::Base')) {
        $output = $output->frame_wrap;
    }
    $output->channel($id || 0);

    if ($self->verbose) {
        print STDERR '[C] --> [S] ', Dumper($output), "\n";
    }
    print {$self->_socket} $output->to_raw_frame();

    return;
}

sub DEMOLISH {
    my ($self) = @_;

    $self->close();
    return;
}

1;
__END__

=head1 NAME

RabbitFoot - A synchronous and single channel Perl AMQP client.

=head1 SYNOPSIS

  use RabbitFoot;

  my $rf = RabbitFoot->new({
      timeout => 1,
  })->load_xml_spec(
      '/path/to/amqp0-8.xml',
  )->connect({
      host  => 'localhosti',
      port  => 5672,
      user  => 'guest',
      port  => 'guest',
      vhost => '/',
  });

  $rf->declare_exchange({exchange => 'test_exchange'});

=head1 DESCRIPTION

RabbitFoot is an AMQP(Advanced Message Queuing Protocol) client library, that is intended to allow you to interact with AMQP-compliant message brokers/servers such as RabbitMQ in a synchronous fashion.

You can use RabbitFoot to -

  * Declare and delete exchanges
  * Declare, delete and bind queues
  * Set QoS
  * Publish, consume, get, ack and recover messages

RabbitFoot is known to work with RabbitMQ versions 1.7.0 and version 0-8 of the AMQP specification.

=head1 AUTHOR

Masahito Ikuta E<lt>cooldaemon@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
