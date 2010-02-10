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
plan tests => 23;

use RabbitFoot;

#my $rf = RabbitFoot->new(timeout => 1, verbose => 1,);
my $rf = RabbitFoot->new(timeout => 1,);

lives_ok sub {
    $rf->load_xml_spec($FindBin::Bin . '/../fixed_amqp0-8.xml')
}, 'load xml spec';
 
lives_ok sub {
    $rf->connect((map {$_ => $conf->{$_}} qw(host port user pass vhost)));
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

my $done = AnyEvent->condvar;
lives_ok sub {
    $ch->consume(
        queue      => 'test_q',
        on_consume => sub {
            my $response = shift;
            $done->send($response->{deliver}->method_frame->consumer_tag);
        },
    );
}, 'consume';

lives_ok sub {
    $ch->cancel(consumer_tag => $done->recv,);
}, 'cancel';

lives_ok sub {
    publish($ch, 'I love RabbitMQ.');
    $ch->get(queue => 'test_q');
}, 'get';

lives_ok sub {
    $ch->get(queue => 'test_q');
}, 'empty';

$done = AnyEvent->condvar;
lives_ok sub {
    $ch->consume(
        queue  => 'test_q',
        no_ack => 0,
        on_consume => sub {
            my $response = shift;
            $ch->ack(
                delivery_tag => $response->{deliver}->method_frame->delivery_tag
            );
            $done->send($response->{deliver}->method_frame->consumer_tag);
        }
    );
    publish($ch, 'NO RabbitMQ, NO LIFE.');
    $ch->cancel(consumer_tag => $done->recv,);
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

    $done = AnyEvent->condvar;
    my @responses;
    my $frame = $ch->consume(
        queue  => 'test_q',
        no_ack => 0,
        on_consume => sub {
            my $response = shift;
            push @responses, $response;
            return if 2 > scalar @responses;
            $done->send;
        },
    );
    publish($ch, 'RabbitMQ is excellent.');
    publish($ch, 'RabbitMQ is fantastic.');
    $done->recv;

    for my $response (@responses) {
        $ch->ack(
            delivery_tag => $response->{deliver}->method_frame->delivery_tag,
        );
    }

    $ch->cancel(consumer_tag => $frame->method_frame->consumer_tag,);
    $ch->qos();
}, 'qos';

lives_ok sub {
    $done = AnyEvent->condvar;
    my $recover_count = 0;
    $ch->consume(
        queue  => 'test_q',
        no_ack => 0,
        on_consume => sub {
            my $response = shift;

            if (5 > ++$recover_count) {
                $ch->recover();
                return;
            }

            $ch->ack(
                delivery_tag => $response->{deliver}->method_frame->delivery_tag
            );

            $done->send($response->{deliver}->method_frame->consumer_tag);
        }
    );
    publish($ch, 'RabbitMQ is powerful.');
    $ch->cancel(consumer_tag => $done->recv,);
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
        on_return   => sub {
            my $response = shift;
            my $error_message = 'on_return: ' . Dumper($response);
            die $error_message;
        },
    );

    return;
}

