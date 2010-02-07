use Test::More tests => 10;

use AnyEvent::RabbitMQ::LocalQueue;

my $q = AnyEvent::RabbitMQ::LocalQueue->new;

$q->push(1);
$q->get(sub {is $_[0], 1, 'push -> get';});

$q->get(sub {is $_[0], 2, 'get -> push';});
$q->push(2);

$q->push(3, 4);
$q->push(5, 6);
$q->get(
    sub {is $_[0], 3, '';},
    sub {is $_[0], 4, '';},
);
$q->get(
    sub {is $_[0], 5, '';},
    sub {is $_[0], 6, '';},
);

$q->get(
    sub {is $_[0], 7, '';},
    sub {is $_[0], 8, '';},
);
$q->get(
    sub {is $_[0], 9, '';},
    sub {is $_[0], 10, '';},
);
$q->push(7, 8);
$q->push(9, 10);

