use inc::Module::Install;
require Module::Install::TestBase;

name 'Net-RabbitFoot';
all_from 'lib/Net/RabbitFoot.pm';

requires 'Moose';
requires 'MooseX::AttributeHelpers';
requires 'MooseX::App::Cmd';
requires 'MooseX::ConfigFromFile';
requires 'Config::Any';
requires 'JSON::XS';
requires 'List::MoreUtils';
requires 'Net::AMQP';
requires 'AnyEvent';
requires 'Coro';
requires 'Coro::AnyEvent';
requires 'File::ShareDir';

tests 't/*.t';
author_tests 'xt';
install_share;

build_requires 'Test::More';
build_requires 'Test::Exception';
install_script 'rabbit_foot';
use_test_base;
auto_include;
WriteAll;
