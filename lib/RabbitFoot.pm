package RabbitFoot;

use AnyEvent::RabbitMQ;
use Coro;
use Coro::AnyEvent;

use RabbitFoot::Channel;

use Moose;

our $VERSION = '0.02';

has verbose => (
    isa => 'Bool',
    is  => 'rw',
);

has _ar => (
    isa     => 'AnyEvent::RabbitMQ',
    is      => 'ro',
);

for my $method (qw(connect close)) {
    __PACKAGE__->meta->add_method($method, sub {
        my $self = shift;
        $self->_do($method, @_);
        return $self;
    });
}

__PACKAGE__->meta->make_immutable;
no Moose;

sub BUILD {
    my $self = shift;
    $self->{_ar} = AnyEvent::RabbitMQ->new(
        verbose => $self->verbose,
    );
}

sub load_xml_spec {
    my $self = shift;
    $self->{_ar}->load_xml_spec(@_);
    return $self;
}

sub open_channel {
    my $self = shift;
    return RabbitFoot::Channel->new(arc => $self->_do('open_channel', @_));
}

sub _do {
    my $self   = shift;
    my $method = shift;
    my %args   = @_;

    my $cb = Coro::rouse_cb;
    $args{on_success} = sub {$cb->(1, @_);},
    $args{on_failure} = sub {$cb->(0, @_);},

    $self->_ar->$method(%args);
    my ($is_success, @responses) = Coro::rouse_wait;
    die @responses if !$is_success;
    return @responses;
}

1;
__END__

=head1 NAME

RabbitFoot - An Asynchronous and single channel Perl AMQP client.

=head1 SYNOPSIS

  use RabbitFoot;

  my $rf = RabbitFoot->new()->load_xml_spec(
      '/path/to/amqp0-8.xml',
  )->connect(
      host    => 'localhosti',
      port    => 5672,
      user    => 'guest',
      port    => 'guest',
      vhost   => '/',
      timeout => 1,
  );

  my $ch = $rf->open_channel();
  $ch->declare_exchange(exchange => 'test_exchange');

=head1 DESCRIPTION

RabbitFoot is an AMQP(Advanced Message Queuing Protocol) client library, that is intended to allow you to interact with AMQP-compliant message brokers/servers such as RabbitMQ in an asynchronous fashion.

You can use RabbitFoot to -

  * Declare and delete exchanges
  * Declare, delete, bind and unbind queues
  * Set QoS
  * Publish, consume, get, ack and recover messages
  * Select, commit and rollback transactions

RabbitFoot is known to work with RabbitMQ versions 1.7.1 and version 0-8 of the AMQP specification.

=head1 AUTHOR

Masahito Ikuta E<lt>cooldaemon@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
