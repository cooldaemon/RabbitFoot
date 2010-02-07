package AnyEvent::RabbitMQ::LocalQueue;

use Moose;
use MooseX::AttributeHelpers;

our $VERSION = '0.01';

has _message_queue => (
    metaclass => 'Collection::Array',
    is        => 'ro',
    isa       => 'ArrayRef[Any]',
    default   => sub {[]},
    provides  => {
        push  => '_push_message_queue',
        shift => '_shift_message_queue',
        count => '_count_message_queue',
    },
);

has _drain_code_queue => (
    metaclass => 'Collection::Array',
    is        => 'ro',
    isa       => 'ArrayRef[CodeRef]',
    default   => sub {[]},
    provides  => {
        push  => '_push_drain_code_queue',
        shift => '_shift_drain_code_queue',
        count => '_count_drain_code_queue',
    },
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub push {
    my $self = shift;
    $self->_push_message_queue(@_);
    return $self->_drain_queue();
}

sub get {
    my $self = shift;
    $self->_push_drain_code_queue(@_);
    return $self->_drain_queue();
}

sub _drain_queue {
    my $self = shift;

    my $count
        = $self->_count_message_queue < $self->_count_drain_code_queue
        ? $self->_count_message_queue : $self->_count_drain_code_queue
        ;

    for (1 .. $count) {
        $self->_shift_drain_code_queue->($self->_shift_message_queue);
    }

    return $self;
}

1;

