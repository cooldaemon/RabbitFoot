use Test::More;
use Test::Exception;

use FindBin;
use JSON::XS;

my $json_text;
open my $fh, '<', $FindBin::Bin . '/../config.json' or die $!;
{undef $/; $json_text = <$fh>;}
close $fh;
my $conf = decode_json($json_text);

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
plan tests => 23;

use Coro;
use Net::RabbitFoot;

my $rf = Net::RabbitFoot->new();

lives_ok sub {
    $rf->load_xml_spec(Net::RabbitFoot::default_amqp_spec())
}, 'load xml spec';

lives_ok sub {
    $rf->connect(
        (map {$_ => $conf->{$_}} qw(host port user pass vhost)),
        timeout => 1,
    );
}, 'connect';

my $ch;
lives_ok sub {$ch = $rf->open_channel();}, 'open channel';

lives_ok sub {
    $ch->declare_exchange(exchange => 'test_x');
}, 'declare_exchange';

lives_ok sub {
    $ch->declare_queue(queue => 'test_q');
}, 'declare_queue';

lives_ok sub {
    $ch->bind_queue(
        queue       => 'test_q',
        exchange    => 'test_x',
        routing_key => 'test_r',
    );
}, 'bind_queue';

lives_ok sub {
    publish($ch, 'Hello RabbitMQ.');
}, 'publish';

my $main = $Coro::current;
my $frame;
my $done = 0;
lives_ok sub {
    $frame = $ch->consume(
        queue      => 'test_q',
        on_consume => unblock_sub {
            my $response = shift;
            $done = 1;
            $main->ready;
            schedule;
        },
    );
}, 'consume';

schedule while !$done;

lives_ok sub {
    $ch->cancel(consumer_tag => $frame->method_frame->consumer_tag);
}, 'cancel';

lives_ok sub {
    publish($ch, 'I love RabbitMQ.');
    $ch->get(queue => 'test_q');
}, 'get';

lives_ok sub {
    $ch->get(queue => 'test_q');
}, 'empty';

lives_ok sub {
    $done = 0;
    $frame = $ch->consume(
        queue      => 'test_q',
        no_ack     => 0,
        on_consume => unblock_sub {
            my $response = shift;
            $ch->ack(
                delivery_tag => $response->{deliver}->method_frame->delivery_tag
            );

            $done = 1;
            $main->ready;
            schedule;
        }
    );

    publish($ch, 'NO RabbitMQ, NO LIFE.');
    schedule while !$done;
    $ch->cancel(consumer_tag => $frame->method_frame->consumer_tag);

}, 'ack deliver';

lives_ok sub {
    publish($ch, 'RabbitMQ is cool.');
    my $response = $ch->get(
        queue  => 'test_q',
        no_ack => 0,
    );
    $ch->ack(delivery_tag => $response->{ok}->method_frame->delivery_tag,);
}, 'ack get';

lives_ok sub {
    $ch->qos(prefetch_count => 2);

    my @responses;
    $done = 0;
    $frame = $ch->consume(
        queue  => 'test_q',
        no_ack => 0,
        on_consume => unblock_sub {
            my $response = shift;
            push @responses, $response;
            return if 2 > scalar @responses;

            $done = 1;
            $main->ready;
            schedule;
        },
    );
    publish($ch, 'RabbitMQ is excellent.');
    publish($ch, 'RabbitMQ is fantastic.');
    schedule while !$done;

    for my $response (@responses) {
        $ch->ack(
            delivery_tag => $response->{deliver}->method_frame->delivery_tag,
        );
    }

    $ch->cancel(consumer_tag => $frame->method_frame->consumer_tag,);
    $ch->qos();
}, 'qos';

lives_ok sub {
    my $recover_count = 0;
    $done = 0;
    $frame = $ch->consume(
        queue  => 'test_q',
        no_ack => 0,
        on_consume => unblock_sub {
            my $response = shift;

            if (5 > ++$recover_count) {
                $ch->recover();
                return;
            }

            $ch->ack(
                delivery_tag => $response->{deliver}->method_frame->delivery_tag
            );

            $done = 1;
            $main->ready;
            schedule;
        }
    );
    publish($ch, 'RabbitMQ is powerful.');
    schedule while !$done;
    $ch->cancel(consumer_tag => $frame->method_frame->consumer_tag);
}, 'recover';

lives_ok sub {
    $ch->select_tx();
}, 'select_tx';

lives_ok sub {
    publish($ch, 'RabbitMQ is highly reliable systems.');
    $ch->rollback_tx();
}, 'rollback_tx';

lives_ok sub {
    publish($ch, 'RabbitMQ is highly reliable systems.');
    $ch->commit_tx();
}, 'commit_tx';

lives_ok sub {
    $ch->purge_queue(queue => 'test_q');
}, 'purge_queue';

lives_ok sub {
    $ch->unbind_queue(
        queue       => 'test_q',
        exchange    => 'test_x',
        routing_key => 'test_r',
    );
}, 'unbind_queue';

lives_ok sub {
    $ch->delete_queue(queue => 'test_q');
}, 'delete_queue';

lives_ok sub {
    $ch->delete_exchange(exchange => 'test_x');
}, 'delete_exchange';

lives_ok sub {
    $rf->close();
}, 'close';

sub publish {
    my ($ch, $message,) = @_;

    $ch->publish(
        exchange    => 'test_x',
        routing_key => 'test_r',
        body        => $message,
        on_return   => unblock_sub {
            my $response = shift;
            my $error_message = 'on_return: ' . Dumper($response);
            die $error_message;
        },
    );

    return;
}

