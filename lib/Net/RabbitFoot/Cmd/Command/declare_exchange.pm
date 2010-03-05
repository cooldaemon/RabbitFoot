package Net::RabbitFoot::Cmd::Command::declare_exchange;

use List::MoreUtils qw(none);
use Moose;

extends qw(MooseX::App::Cmd::Command);
with qw(Net::RabbitFoot::Cmd::Role::Config Net::RabbitFoot::Cmd::Role::Command);

has exchange => (
    isa           => 'Str',
    is            => 'rw',
    default       => '',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'e',
    documentation => 'exchange name',
);

has type => (
    isa           => 'Str',
    is            => 'rw',
    default       => 'direct',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 't',
    documentation => 'exchange type [direct|topic|fanout|headers]',
);

has passive => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
#   cmd_aliases   => 'p',
    documentation => 'do not create exchange',
);

has durable => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'd',
    documentation => 'request a durable exchange',
);

has auto_delete => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'a',
    documentation => 'auto delete exchange when unused',
);

has internal => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'i',
    documentation => 'create internal exchange',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub abstract {
    return 'declare exchange, create if needed';
}

sub _validate_exchange {
    my ($self,) = @_;

    return if !$self->exchange;
    $self->_check_shortstr('exchange');
    return;
}

sub _validate_type {
    my ($self,) = @_;

    die 'type', "\n"
        if none {$_ eq $self->type} qw(direct topic fanout headers);
    return;
}

sub _run {
    my ($self, $client, $opt, $args,) = @_;

    my $method_frame = $client->declare_exchange(
        (map {$_ => $self->$_} qw(exchange type passive durable auto_delete internal))
    )->method_frame;

    print 'Declared exchange', "\n";
    return;
} 

1;

