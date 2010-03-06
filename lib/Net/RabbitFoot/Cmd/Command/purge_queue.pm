package Net::RabbitFoot::Cmd::Command::purge_queue;

use Moose;
extends qw(MooseX::App::Cmd::Command);
with qw(Net::RabbitFoot::Cmd::Role::Config Net::RabbitFoot::Cmd::Role::Command);

has queue => (
    isa           => 'Str',
    is            => 'rw',
    required      => 1,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'q',
    documentation => 'queue name',
);

__PACKAGE__->meta->make_immutable;
no Moose;

sub abstract {
    return 'purge a queue';
}

sub _validate_queue {
    my ($self,) = @_;

    $self->_check_queue();
    return;
}

sub _run {
    my ($self, $client, $opt, $args,) = @_;

    my $method_frame = $client->purge_queue(queue => $self->queue)->method_frame;

    print 'Purged queue', "\n";
    print 'message_count: ', $method_frame->message_count, "\n";
    return;
} 

1;

