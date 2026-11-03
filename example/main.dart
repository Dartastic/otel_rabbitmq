// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_rabbitmq/otel_rabbitmq.dart';

/// Traces Rabbitmq calls with OpenTelemetry.
///
/// `otel_rabbitmq` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Rabbitmq call in
/// [tracedRabbitmqPublish]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'rabbitmq-example');

  // Wraps a RabbitMQ publish in a PRODUCER span using OTel messaging semconv (messaging.system=rabbitmq).
  final rabbitmqPublishId = await tracedRabbitmqPublish<String>(
    exchange: 'example',
    routingKey: 'orders/2026/inv-1042.pdf',
    payloadSize: 512,
    messageId: 'message_8f3a2c',
    mandatory: true,
    invoke: () async {
      // Make the Rabbitmq call here, then report what it returned.
      return 'rabbitmq_8f3a2c';
    },
  );
  OTelLog.info('tracedRabbitmqPublish returned $rabbitmqPublishId');

  // Wraps the user's handling of one RabbitMQ delivery in a CONSUMER span.
  final rabbitmqConsumeId = await tracedRabbitmqConsume<String>(
    queue: 'orders',
    consumerTag: 'example',
    deliveryTag: 1,
    redelivered: false,
    messageId: 'message_8f3a2c',
    invoke: () async {
      // Make the Rabbitmq call here, then report what it returned.
      return 'rabbitmq_8f3a2c';
    },
  );
  OTelLog.info('tracedRabbitmqConsume returned $rabbitmqConsumeId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutRabbitmqInstrumentationAsync.
  await runWithoutRabbitmqInstrumentationAsync(
    () => tracedRabbitmqPublish<String>(
      exchange: 'example',
      routingKey: 'orders/2026/inv-1042.pdf',
      payloadSize: 512,
      messageId: 'message_8f3a2c',
      mandatory: true,
      invoke: () async {
        return 'rabbitmq_8f3a2c';
      },
    ),
  );

  await OTel.shutdown();
}
