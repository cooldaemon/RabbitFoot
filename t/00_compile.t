use strict;
use Test::More tests => 8;

BEGIN {
    use_ok 'RabbitFoot';
    use_ok 'RabbitFoot::Cmd';
    use_ok 'RabbitFoot::Cmd::Role::Command';
    use_ok 'RabbitFoot::Cmd::Role::Config';
    use_ok 'RabbitFoot::Cmd::Command::declare_queue';
    use_ok 'RabbitFoot::Cmd::Command::bind_queue';
    use_ok 'RabbitFoot::Cmd::Command::purge_queue';
    use_ok 'RabbitFoot::Cmd::Command::declare_exchange';
}
