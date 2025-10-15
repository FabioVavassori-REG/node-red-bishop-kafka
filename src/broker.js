module.exports = function (RED) {
	const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;
	const fs = require('fs');

	const dictLogLevel = {
		none: logLevel.NOTHING,
		error: logLevel.ERROR,
		warn: logLevel.WARN,
		info: logLevel.INFO,
		debug: logLevel.DEBUG,
	};

	function KafkaBrokerNode(config) {
		RED.nodes.createNode(this, config);

		let node = this;

		/** @type {KafkaConfig} */
		const options = {
			brokers: config.brokers.replace(' ', '').split(','),
			clientId: config.clientid,
			logLevel: dictLogLevel[config.loglevel],
			connectionTimeout: parseInt(config.connectiontimeout),
			requestTimeout: parseInt(config.requesttimeout),
			retry: null,
		};

		if (config.advancedretry) {
			options.retry = options.retry || {};
			Object.assign(options.retry, {
				maxRetryTime: parseInt(config.maxretrytime),
				initialRetryTime: parseInt(config.initialretrytime),
				factor: parseFloat(config.factor),
				multiplier: parseInt(config.multiplier),
				retries: parseInt(config.retries),
			});
		}

		if (config.auth == 'tls') {
			options.ssl = {};
			Object.assign(options.ssl, {
				ca: config.tlscacert ? fs.readFileSync(config.tlscacert, 'utf-8') : undefined,
				cert: config.tlsclientcert ? fs.readFileSync(config.tlsclientcert, 'utf-8') : undefined,
				key: config.tlsprivatekey ? fs.readFileSync(config.tlsprivatekey, 'utf-8') : undefined,
				passphrase: config.tlspassphrase ?? undefined
			});
		} else if (config.auth == 'sasl') {
			options.ssl = config.saslssl ? { rejectUnauthorized: !config.saslselfsign } : false;

			options.sasl = options.sasl || {};
			Object.assign(options.sasl, {
				mechanism: config.saslmechanism || 'plain',
				username: node.credentials.saslusername,
				password: node.credentials.saslpassword,
			});
		}

		node.server = new Kafka({
			kafkaJS: options
		})
	}

	RED.nodes.registerType('bishop-kafka-broker', KafkaBrokerNode, {
		credentials: {
			saslusername: { type: "text" },
			saslpassword: { type: "password" }
		}
	});
};
