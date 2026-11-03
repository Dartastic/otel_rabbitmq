// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_rabbitmq/otel_rabbitmq.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_rabbitmq-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('publish records PRODUCER span', () async {
    await tracedRabbitmqPublish<String>(
      exchange: 'orders.exchange',
      routingKey: 'orders.new',
      payloadSize: 256,
      messageId: 'm-1',
      mandatory: true,
      invoke: () async => 'ok',
    );
    final a = attrsOf(spans.findSpanByName('publish orders.exchange')!);
    expect(a['messaging.system'], 'rabbitmq');
    expect(a['messaging.destination.name'], 'orders.exchange');
    expect(a['messaging.rabbitmq.routing_key'], 'orders.new');
    expect(a['messaging.message.body.size'], 256);
    expect(a['messaging.message.id'], 'm-1');
    expect(a['messaging.rabbitmq.mandatory'], true);
  });

  test('consume records CONSUMER span', () async {
    await tracedRabbitmqConsume<String>(
      queue: 'orders.queue',
      consumerTag: 'ctag-1',
      deliveryTag: 99,
      redelivered: false,
      messageId: 'm-1',
      invoke: () async => 'processed',
    );
    final a = attrsOf(spans.findSpanByName('receive orders.queue')!);
    expect(a['messaging.system'], 'rabbitmq');
    expect(a['messaging.destination.name'], 'orders.queue');
    expect(a['messaging.rabbitmq.consumer_tag'], 'ctag-1');
    expect(a['messaging.rabbitmq.delivery_tag'], 99);
    expect(a['messaging.rabbitmq.redelivered'], false);
  });

  test('suppression skips spans', () async {
    await runWithoutRabbitmqInstrumentationAsync(() async {
      await tracedRabbitmqPublish<String>(
        exchange: 'x',
        routingKey: 'r',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
  });
}
