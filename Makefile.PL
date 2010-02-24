use inc::Module::Install;
name 'RabbitFoot';
all_from 'lib/RabbitFoot.pm';

requires 'Moose';
requires 'MooseX::AttributeHelpers';
requires 'MooseX::App::Cmd';
requires 'MooseX::ConfigFromFile';
requires 'Config::Any';
requires 'JSON::Syck' => '0.88';
requires 'List::MoreUtils';
requires 'Net::AMQP';
requires 'AnyEvent';
requires 'Coro';
requires 'Coro::AnyEvent';
requires 'File::ShareDir';

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::More';
build_requires 'Test::Exception';
install_script 'rabbit_foot';
use_test_base;
auto_include;
WriteAll;
