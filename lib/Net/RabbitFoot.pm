package Net::RabbitFoot;

use strict;
use warnings;

use AnyEvent::RabbitMQ;
use Coro;
use Coro::AnyEvent;

use Net::RabbitFoot::Channel;

our $VERSION = '1.03';

BEGIN {
    for my $method (qw(connect close)) {
        no strict 'refs';
        *{__PACKAGE__ . '::' . $method} = sub {
            my $self = shift;
            $self->_do($method, @_);
            return $self;
        };
    }
}

sub new {
    my $class = shift;
    return bless {
        _ar => AnyEvent::RabbitMQ->new(@_),
    }, $class;
}

sub ar {
    return shift->{_ar};
}

sub load_xml_spec {
    my $self = shift;
    $self->{_ar}->load_xml_spec(@_);
    return $self;
}

sub server_properties {
    return shift->{_ar}->server_properties();
}

sub open_channel {
    my $self = shift;
    return Net::RabbitFoot::Channel->new(
        arc => $self->_do('open_channel', @_,)
    );
}

sub _do {
    my $self = shift;
    my $method = shift;
    my %args = @_;

    my $cb = Coro::rouse_cb;
    $args{on_success} = sub {$cb->(1, @_);},
    $args{on_failure} = sub {$cb->(0, @_);},

    $self->{_ar}->$method(%args);
    my ($is_success, @responses) = Coro::rouse_wait;
    die @responses if !$is_success;
    return @responses;
}

1;
__END__

=head1 NAME

Net::RabbitFoot - An Asynchronous and multi channel Perl AMQP client.

=head1 SYNOPSIS

  use Net::RabbitFoot;
  use Coro;

  my $rf = Net::RabbitFoot->new()->load_xml_spec()->connect(
      host    => 'localhost',
      port    => 5672,
      user    => 'guest',
      pass    => 'guest',
      vhost   => '/',
      timeout => 1,
  );

  my $ch = $rf->open_channel(on_return => sub { warn "message went nowhere: ".Dumper(shift) });
  $ch->declare_exchange(exchange => 'test_exchange');

  $ch->publish(...);  # same params as AnyEvent::RabbitMQ

  # confirm mode for 0.9
  $ch->confirm;

  my $returned;
  $ch->publish(...,               # normal publish params
               mandatory => 1,    # if it goes nowhere, return it
               on_ack => rouse_cb,
               on_return => sub { $returned = shift });
  rouse_wait;  # wait for ack
  die "the message went nowhere" if $returned;

=head1 DESCRIPTION

Net::RabbitFoot is an AMQP(Advanced Message Queuing Protocol) client library, that is intended to allow you to interact with AMQP-compliant message brokers/servers such as RabbitMQ in an asynchronous fashion.

You can use Net::RabbitFoot to -

  * Declare and delete exchanges
  * Declare, delete, bind and unbind queues
  * Set QoS
  * Publish, consume, get, ack and recover messages
  * Select, commit and rollback transactions
  * Set publish confirm mode and get callbacks on publish ack and return (AMQP 0.9)

Net::RabbitFoot is known to work with RabbitMQ versions 2.3.1 through 2.7.1 and versions 0-8 and 0-9-1 of the AMQP specification.

=head1 BUGS

Documentation would be nice.

=head1 AUTHOR

Masahito Ikuta E<lt>cooldaemon@gmail.comE<gt>

=head1 COPYRIGHT

Copyright (c) 2010, the above named author(s).

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
