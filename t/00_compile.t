use strict;
use Test::More tests => 15;

BEGIN {
    use_ok 'Net::RabbitFoot';
    use_ok 'Net::RabbitFoot::Channel';

    use_ok 'Net::RabbitFoot::Cmd';
    use_ok 'Net::RabbitFoot::Cmd::Role::Command';
    use_ok 'Net::RabbitFoot::Cmd::Role::Config';
    use_ok 'Net::RabbitFoot::Cmd::Command::declare_queue';
    use_ok 'Net::RabbitFoot::Cmd::Command::delete_queue';
    use_ok 'Net::RabbitFoot::Cmd::Command::bind_queue';
    use_ok 'Net::RabbitFoot::Cmd::Command::unbind_queue';
    use_ok 'Net::RabbitFoot::Cmd::Command::purge_queue';
    use_ok 'Net::RabbitFoot::Cmd::Command::declare_exchange';
    use_ok 'Net::RabbitFoot::Cmd::Command::delete_exchange';

    use_ok 'AnyEvent::RabbitMQ';
    use_ok 'AnyEvent::RabbitMQ::Channel';
    use_ok 'AnyEvent::RabbitMQ::LocalQueue';
}
