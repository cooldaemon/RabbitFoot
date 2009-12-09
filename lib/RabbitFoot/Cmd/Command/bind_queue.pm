package RabbitFoot::Cmd::Command::bind_queue;

use Moose;
extends qw(MooseX::App::Cmd::Command);
with qw(RabbitFoot::Cmd::Role::Config RabbitFoot::Cmd::Role::Command);

has queue => (
    isa           => 'Str',
    is            => 'rw',
    required      => 1,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'q',
    documentation => 'queue name',
);

has exchange => (
    isa           => 'Str',
    is            => 'rw',
    required      => 1,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'e',
    documentation => 'exchange name',
);

has routing_key => (
    isa           => 'Str',
    is            => 'rw',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'r',
    documentation => 'message routing key',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub abstract {
    return 'bind queue to an exchange';
}

sub _validate_queue {
    my ($self,) = @_;

    $self->_check_queue();
    return;
}

sub _validate_exchange {
    my ($self,) = @_;

    $self->_check_shortstr('exchange');
    return;
}

sub _validate_routing_key {
    my ($self,) = @_;

    return if !$self->routing_key;
    die 'routing_key', "\n" if 255 < length($self->routing_key);
    return;
}

sub _run {
    my ($self, $client, $opt, $args,) = @_;

    my $method_frame = $client->bind_queue({
        (map {$_ => $self->$_} qw(queue exchange routing_key))
    })->method_frame;

    print 'Bound queue to exchange', "\n";
    return;
} 

1;

