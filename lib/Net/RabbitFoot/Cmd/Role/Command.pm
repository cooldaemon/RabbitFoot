package Net::RabbitFoot::Cmd::Role::Command;

use FindBin;
use Net::RabbitFoot;

use Moose::Role;
use Carp qw/ confess /;
requires qw(_run);

has spec => (
    isa => 'Str',
    is  => 'rw',
    default       => '',
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

    my $rf_closed = AnyEvent->condvar;
    my $rf = Net::RabbitFoot->new(
        verbose => $self->verbose,
    )->load_xml_spec(
        $self->spec,
    )->connect(
        (map {$_ => $self->$_} qw(host port user pass vhost)),
        timeout  => 5,
        on_close => sub {
            my $reply = shift;
            my $w; $w = AnyEvent->idle(cb => sub {
                undef $w;
                $self->_close($reply);
                $rf_closed->send;
            });
        },
    );

    my $ch_closed = AnyEvent->condvar;
    my $ch = $rf->open_channel(
        on_close => sub {
            my $reply = shift;
            my $w; $w = AnyEvent->idle(cb => sub {
                undef $w;
                $self->_close($reply);
                $ch_closed->send;
                $rf->close;
            });
        },
    );

    $self->_run($ch, @_,);

    $ch->close;
    $rf->close;
    $ch_closed->recv;
    $rf_closed->recv;
    return;
}

sub _close {
    my ($self, $reply) = @_;
    confess "No reply" unless $reply;
    my $method_frame = $reply->method_frame;
    print $method_frame->reply_code, ' ', $method_frame->reply_text, "\n";
    return;
}

1;

