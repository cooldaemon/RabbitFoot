package RabbitFoot::Cmd::Command::delete_queue;

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

has if_unused => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'U',
    documentation => 'delete only if unused',
);

has if_empty => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'e',
    documentation => 'delete only if empty',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub abstract {
    return 'delete a queue';
}

sub _validate_queue {
    my ($self,) = @_;

    $self->_check_queue();
    return;
}

sub _run {
    my ($self, $client, $opt, $args,) = @_;

    my $method_frame = $client->delete_queue(
        (map {$_ => $self->$_} qw(queue if_unused if_empty))
    )->method_frame;

    print 'Deleted queue', "\n";
    print 'message_count: ', $method_frame->message_count, "\n";
    return;
} 

1;

