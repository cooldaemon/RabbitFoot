use inc::Module::Install;
name 'RabbitFoot';
all_from 'lib/RabbitFoot.pm';

requires 'Moose';
requires 'MooseX::AttributeHelpers';
requires 'MooseX::App::Cmd';
requires 'MooseX::ConfigFromFile';
requires 'Config::Any';
requires 'JSON::Syck';
requires 'List::MoreUtils';
requires 'Sys::SigAction';
requires 'Net::AMQP';
requires 'AnyEvent';

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::More';
build_requires 'Test::Exception';
use_test_base;
auto_include;
WriteAll;
