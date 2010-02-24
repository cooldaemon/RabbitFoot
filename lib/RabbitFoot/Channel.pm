package RabbitFoot::Channel;

use strict;
use warnings;

use Coro;
use Coro::AnyEvent;

use AnyEvent::RabbitMQ::Channel;

our $VERSION = '1.00';

BEGIN {
    for my $method (qw(
        close
        declare_exchange delete_exchange
        declare_queue bind_queue unbind_queue purge_queue delete_queue
        consume cancel get qos
        select_tx commit_tx rollback_tx
    )) {
        no strict 'refs';
        *{__PACKAGE__ . '::' . $method} = sub {
            my $self = shift;
            my %args = @_;

            my $cb = Coro::rouse_cb;
            $args{on_success} = sub {$cb->(1, @_);},
            $args{on_failure} = sub {$cb->(0, @_);},

            $self->{arc}->$method(%args);
            my ($is_success, @responses) = Coro::rouse_wait;
            die @responses if !$is_success;
            return $responses[0];
        };
    }

    for my $method (qw(publish ack recover)) {
        no strict 'refs';
        *{__PACKAGE__ . '::' . $method} = sub {
            my $self = shift;
            $self->{arc}->$method(@_);
            return $self;
        };
    }
}

sub new {
    my $class = shift;
    return bless {
        @_, # arc
    }, $class;
}

1;
