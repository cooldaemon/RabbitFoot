use Test::More;
use Test::Exception;

use FindBin;
use JSON::Syck;

my $conf = JSON::Syck::LoadFile($FindBin::Bin . '/../config.json');

eval {
    use IO::Socket::INET;

    my $socket = IO::Socket::INET->new(
        Proto    => 'tcp',
        PeerAddr => $conf->{host},
        PeerPort => $conf->{port},
        Timeout  => 1,
    ) or die 'Error connecting to AMQP Server!';

    close $socket;
};

plan skip_all => 'Connection failure: '
               . $conf->{host} . ':' . $conf->{port} if $@;
plan tests => 6;

use Coro;
use RabbitFoot;

my $rf = RabbitFoot->new()->load_xml_spec(
    RabbitFoot::default_amqp_spec()
)->connect(
    (map {$_ => $conf->{$_}} qw(host port user pass vhost)),
    timeout => 1,
);

my $main = $Coro::current;
my $done = 0;

my @queues = map {
    my $queue = 'test_q' . $_;
    my $ch = $rf->open_channel();
    isa_ok($ch, 'RabbitFoot::Channel');

    $ch->declare_queue(queue => $queue);

    my $frame; $frame = $ch->consume(
        queue      => $queue,
        on_consume => unblock_sub {
            my $response = shift;
            return if 'stop' ne $response->{body}->payload;

            $ch->cancel(consumer_tag => $frame->method_frame->consumer_tag);
            $done++;
            $main->ready;
            schedule;
        },
    );

    $queue;
} (1 .. 5);

my $ch = $rf->open_channel();
for my $queue (@queues) {
    publish($ch, $queue, 'Hello Coro.');
    publish($ch, $queue, 'stop');
}
schedule while $done < 5;

is($done, 5, 'consume count');

$ch->delete_queue(queue => $_) for @queues;

$rf->close;

sub publish {
    my ($ch, $queue, $message,) = @_;

    $ch->publish(
        routing_key => $queue,
        body        => $message,
        mandatory   => 1,
        on_return   => unblock_sub {die Dumper(@_);},
    );

    return;
}

