package Net::RabbitFoot::Cmd::Role::Config;

use FindBin;
use Config::Any;

use Moose::Role;
with qw(MooseX::ConfigFromFile);

has configfile => (
    isa => 'Str',
    is  => 'rw',
    default       => $FindBin::Bin . '/config.json',
    metaclass     => 'MooseX::Getopt::Meta::Attribute',
    cmd_aliases   => 'c',
    documentation => 'config file',
);

no Moose::Role;

sub get_config_from_file {
    my ($self, $file,) = @_;

    return {} if !$file || !-f $file;

    my $config = Config::Any->load_files({
        files       => [$file],
        use_ext     => 1,
        driver_args => {General => {-LowerCaseNames => 1}}
    });

    return $config->[0]->{$file} or die "Could not load $file";
}

1;

