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
plan tests => 17;

use RabbitFoot;

my $rf = RabbitFoot->new({timeout => 1, verbose => 1,});

lives_ok sub {
    $rf->load_xml_spec($FindBin::Bin . '/../amqp0-8.xml')
}, 'load xml spec';
 
lives_ok sub {
    $rf->connect({(map {$_ => $conf->{$_}} qw(host port user pass vhost))});
}, 'connect';

lives_ok sub {
    $rf->declare_exchange({exchange => 'test_x'});
}, 'declare_exchange';

lives_ok sub {
    $rf->declare_queue({queue => 'test_q'});
}, 'declare_queue';

lives_ok sub {
    $rf->bind_queue({
        queue       => 'test_q',
        exchange    => 'test_x',
        routing_key => 'test_r',
    });
}, 'bind_queue';

lives_ok sub {
    publish($rf, 'Hello RabbitMQ.');
}, 'publish for consume';

lives_ok sub {
    $rf->consume({queue => 'test_q'});
}, 'consume';

lives_ok sub {
    $rf->poll({timeout => 1});
}, 'poll';

lives_ok sub {
    $rf->cancel();
}, 'cancel';

lives_ok sub {
    publish($rf, 'I love RabbitMQ.');
    $rf->get({queue => 'test_q'});
}, 'get';

lives_ok sub {
    $rf->get({queue => 'test_q'});
}, 'get empty';

lives_ok sub {
    publish($rf, 'NO RabbitMQ, NO LIFE.');
    $rf->consume({
        queue  => 'test_q',
        no_ack => 0,
    });
    my $response = $rf->poll({timeout => 1});
    $rf->ack({
        delivery_tag => $response->{deliver}->method_frame->delivery_tag,
    });
    $rf->cancel();
}, 'ack deliver';

lives_ok sub {
    publish($rf, 'RabbitMQ is cool.');
    my $response = $rf->get({
        queue  => 'test_q',
        no_ack => 0,
    });
    $rf->ack({
        delivery_tag => $response->{getok}->method_frame->delivery_tag,
    });
}, 'ack get';

lives_ok sub {
    $rf->purge_queue({queue => 'test_q'});
}, 'purge_queue';

lives_ok sub {
    $rf->delete_queue({queue => 'test_q'});
}, 'delete_queue';

lives_ok sub {
    $rf->delete_exchange({exchange => 'test_x'});
}, 'delete_exchange';

lives_ok sub {
    $rf->close();
}, 'close';

sub publish {
    my ($rf, $message,) = @_;

    $rf->publish(
        {
            exchange    => 'test_x',
            routing_key => 'test_r',
        },
        {},
        $message,
    );

    return;
}

