# Changelog

## [v0.4.1](https://github.com/fujiwara/mqsubscriber/compare/v0.4.0...v0.4.1) - 2026-03-26
- Add log_header_fields option for handler logging by @fujiwara in https://github.com/fujiwara/mqsubscriber/pull/13

## [v0.4.0](https://github.com/fujiwara/mqsubscriber/compare/v0.3.1...v0.4.0) - 2026-03-26
- Add match_pattern option for AMQP topic-style wildcard matching by @fujiwara in https://github.com/fujiwara/mqsubscriber/pull/11

## [v0.3.1](https://github.com/fujiwara/mqsubscriber/compare/v0.3.0...v0.3.1) - 2026-03-25
- Rename request.routing_keys to request.routing_key by @fujiwara in https://github.com/fujiwara/mqsubscriber/pull/9

## [v0.3.0](https://github.com/fujiwara/mqsubscriber/compare/v0.2.3...v0.3.0) - 2026-03-25
- Document response.reply_to mode and fix README inconsistencies by @fujiwara in https://github.com/fujiwara/mqsubscriber/pull/6
- Inherit parent process env and add per-handler env config by @fujiwara in https://github.com/fujiwara/mqsubscriber/pull/8

## [v0.2.3](https://github.com/fujiwara/mqsubscriber/compare/v0.2.2...v0.2.3) - 2026-03-25

## [v0.2.2](https://github.com/fujiwara/mqsubscriber/commits/v0.2.2) - 2026-03-25
- Add RabbitMQ support and rename to mqsubscriber by @fujiwara in https://github.com/fujiwara/mqsubscriber/pull/2
- Bump github.com/fujiwara/mqbridge from 0.1.0 to 0.3.1 by @dependabot[bot] in https://github.com/fujiwara/mqsubscriber/pull/4
- Bump github.com/fujiwara/simplemq-cli from 0.7.1 to 0.8.0 by @dependabot[bot] in https://github.com/fujiwara/mqsubscriber/pull/3

## [v0.2.2](https://github.com/fujiwara/simplemq-subscriber/compare/v0.2.1...v0.2.2) - 2026-03-24
- Drain queue continuously instead of one poll per tick by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/20

## [v0.2.1](https://github.com/fujiwara/simplemq-subscriber/compare/v0.2.0...v0.2.1) - 2026-03-24
- Log command stderr line by line by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/18

## [v0.2.0](https://github.com/fujiwara/simplemq-subscriber/compare/v0.1.1...v0.2.0) - 2026-03-23
- Fix graceful shutdown returning context canceled error by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/15
- Add log_message and log_body_fields to handler config by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/17

## [v0.1.1](https://github.com/fujiwara/simplemq-subscriber/compare/v0.1.0...v0.1.1) - 2026-03-23
- Improve logging visibility for message processing by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/12

## [v0.1.1](https://github.com/fujiwara/simplemq-subscriber/compare/v0.1.0...v0.1.1) - 2026-03-23
- Improve logging visibility for message processing by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/12

## [v0.1.0](https://github.com/fujiwara/simplemq-subscriber/compare/v0.0.3...v0.1.0) - 2026-03-22
- Update README: architecture diagrams, section order, and observability details by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/6
- Add end-to-end RPC example with Docker Compose by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/8
- Make response queue optional and default response to false by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/9
- Add response_ignore option to suppress response by exit code by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/11

## [v0.1.0](https://github.com/fujiwara/simplemq-subscriber/compare/v0.0.3...v0.1.0) - 2026-03-13
- Update README: architecture diagrams, section order, and observability details by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/6
- Add end-to-end RPC example with Docker Compose by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/8
- Make response queue optional and default response to false by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/9

## [v0.0.3](https://github.com/fujiwara/simplemq-subscriber/compare/v0.0.2...v0.0.3) - 2026-03-12
- Add RPC response routing and response status headers by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/5

## [v0.0.2](https://github.com/fujiwara/simplemq-subscriber/compare/v0.0.1...v0.0.2) - 2026-03-12
- Use sscli.SecretNativeFunction instead of duplicated code by @fujiwara in https://github.com/fujiwara/simplemq-subscriber/pull/2

## [v0.0.1](https://github.com/fujiwara/simplemq-subscriber/commits/v0.0.1) - 2026-03-12
