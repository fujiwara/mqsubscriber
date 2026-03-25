{
  simplemq: { api_url: 'http://simplemq-localserver:18080' },
  request: { queue: 'smq-request', api_key: 'test-key', polling_interval: '1s' },
  response: { queue: 'smq-response', api_key: 'test-key' },
  handlers: [
    { name: 'upper', match: { 'rabbitmq.routing_key': 'upper' }, command: ['tr', 'a-z', 'A-Z'], response: true },
  ],
}
