package RabbitFoot::Cmd::Command::declare_queue;

use Moose;
extends qw(MooseX::App::Cmd::Command);
with qw(RabbitFoot::Cmd::Role::Config RabbitFoot::Cmd::Role::Command);

has queue => (
    isa           => 'Str',
    is            => 'rw',
    default       => '',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'q',
    documentation => 'queue name',
);

has passive => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
#    cmd_aliases   => 'p',
    documentation => 'do not create queue',
);

has durable => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'd',
    documentation => 'request a durable queue',
);

has exclusive => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'e',
    documentation => 'request an exclusive queue',
);

has auto_delete => (
    isa           => 'Bool',
    is            => 'rw',
    default       => 0,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'a',
    documentation => 'auto delete queue when unused',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub abstract {
    return 'declare queue, create if needed';
}

sub _validate_queue {
    my ($self,) = @_;

    return if !$self->queue;
    $self->_check_queue();
    return;
}

sub _run {
    my ($self, $client, $opt, $args,) = @_;

    my $method_frame = $client->declare_queue(
        (map {$_ => $self->$_} qw(queue passive durable exclusive auto_delete))
    )->method_frame;

    print 'Declared queue', "\n";
    for my $method (qw(queue message_count consumer_count)) {
        print $method, ': ', $method_frame->$method, "\n";
    }
    return;
} 

1;

