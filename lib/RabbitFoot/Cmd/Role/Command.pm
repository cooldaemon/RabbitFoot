package RabbitFoot::Cmd::Role::Command;

use FindBin;
use RabbitFoot;

use Moose::Role;
requires qw(_run);

has spec => (
    isa => 'Str',
    is  => 'rw',
    default       => $FindBin::Bin . '/amqp0-8.xml',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 's',
    documentation => 'AMQP specification',
);

has host => (
    isa => 'Str',
    is  => 'rw',
    default       => 'localhost',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'H',
    documentation => 'host name or ip address',
);

has port => (
    isa => 'Int',
    is  => 'rw',
    default       => 5672,
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'P',
    documentation => 'port number',
);

has user => (
    isa => 'Str',
    is  => 'rw',
    default       => 'guest',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'u',
    documentation => 'user name',
);

has pass => (
    isa => 'Str',
    is  => 'rw',
    default       => 'guest',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'p',
    documentation => 'password',
);

has vhost => (
    isa => 'Str',
    is  => 'rw',
    default       => '/',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'v',
    documentation => 'virtual host',
);

has verbose => (
    isa => 'Bool',
    is  => 'rw',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'V',
    documentation => 'Verbose mode',
);

no Moose::Role;

sub validate_args {
    my ($self, $opt, $args) = @_;

    for my $method ($self->meta->get_all_methods) {
        next if $method->name !~ /^_validate_/;
        eval {$method->execute($self);};
        die $self->usage_error($@) if $@;
    }
}

sub _validate_spec {
    my ($self,) = @_;

    die 'spec', "\n" if !-f $self->spec;
}

sub _validate_vhost {
    my ($self,) = @_;

    die 'vhost', "\n"
        if    255 < length($self->vhost)
           || $self->vhost !~ m{^[a-zA-Z0-9/\-_]+$};
}

sub _check_queue {
    my ($self,) = @_;

    die 'queue', "\n"
        if    255 < length($self->queue)
           || $self->queue !~ m{^[a-zA-Z0-9/\-_.:=+]+$};
}

sub _check_shortstr {
    my ($self, $arg,) = @_;

    die $arg, "\n"
        if    255 < length($self->$arg)
           || $self->$arg !~ m{^[a-zA-Z0-9-_.:]+$};
}

sub execute {
    my $self = shift;
    my ($opt, $args,) = @_;

    my $client = RabbitFoot->new({
        verbose => $self->verbose,
        timeout => 1,
    })->load_xml_spec(
        $self->spec,
    )->connect({
        (map {$_ => $self->$_} qw(host port user pass vhost))
    });

    $self->_run($client, @_,);
    return;
}

1;

