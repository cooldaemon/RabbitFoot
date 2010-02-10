package RabbitFoot::Channel;

use Coro;
use Coro::AnyEvent;

use AnyEvent::RabbitMQ::Channel;

use Moose;

our $VERSION = '0.01';

has arc => (
    isa => 'AnyEvent::RabbitMQ::Channel',
    is  => 'ro',
);

for my $method (qw(
    close
    declare_exchange delete_exchange
    declare_queue bind_queue unbind_queue purge_queue delete_queue
    consume cancel get qos
    select_tx commit_tx rollback_tx
)) {
    __PACKAGE__->meta->add_method($method, sub {
        my $self = shift;
        my %args = @_;

        my $cb = Coro::rouse_cb;
        $args{on_success} = sub {$cb->(1, @_);},
        $args{on_failure} = sub {$cb->(0, @_);},

        $self->arc->$method(%args);
        my ($is_success, @responses) = Coro::rouse_wait;
        die @responses if !$is_success;
        return $responses[0];
    });
}

for my $method (qw(publish ack recover)) {
    __PACKAGE__->meta->add_method($method, sub {
        my $self = shift;
        $self->arc->$method(@_);
        return $self;
    });
}

__PACKAGE__->meta->make_immutable;
no Moose;

1;
