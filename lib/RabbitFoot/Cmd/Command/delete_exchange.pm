package RabbitFoot::Cmd::Command::delete_exchange;

use Moose;
extends qw(MooseX::App::Cmd::Command);
with qw(RabbitFoot::Cmd::Role::Config RabbitFoot::Cmd::Role::Command);

has exchange => (
    isa           => 'Str',
    is            => 'rw',
    required      => 1,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'q',
    documentation => 'exchange name',
);

has if_unused => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'U',
    documentation => 'delete only if unused',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub abstract {
    return 'delete a exchange';
}

sub _validate_exchange {
    my ($self,) = @_;

    $self->_check_shortstr('exchange');
    return;
}

sub _run {
    my ($self, $client, $opt, $args,) = @_;

    my $method_frame = $client->delete_exchange({
        (map {$_ => $self->$_} qw(exchange if_unused))
    })->method_frame;

    print 'Deleted exchange', "\n";
    return;
} 

1;

