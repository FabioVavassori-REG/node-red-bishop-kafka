/** @import { NodeAPI } from 'node-red' */
/** @typedef { import('@confluentinc/kafka-javascript/types/kafkajs.d.ts').Producer } Producer */
/** @import { KafkaBrokerNode } from './broker' */

/**
 * Module to control the access to the broker.
 * @param {NodeAPI} RED Root node to register the components
 */
module.exports = function (RED) {
	const { Buffer } = require('node:buffer');

	const acksDict = {
		all: -1,
		none: 0,
		leader: 1,
	};

	function KafkaProducerNode(config) {
		const node = this;

		RED.nodes.createNode(this, config);
		this.ready = false;

		/** @type {KafkaBrokerNode} */
		const broker = RED.nodes.getNode(config.broker);
		if (!broker) {
			node.status({ fill: 'red', shape: 'ring', text: 'Broker is missing.' });
			return;
		}

		/** @type {Producer} */
		this.producer = null;

		this.sendOptions = {
			/** @type { string | null } */
			topic: config.topic || null,

			/** @type { string | null } */
			partition: config.partition || null,
			/** @type { string | number | Buffer | null } */
			key: config.key || null,
			/** @type { object } */
			headers: config.headeritems || {},
		};

		this.onConnect = function () {
			node.ready = true;
			node.status({ fill: 'green', shape: 'ring', text: 'Ready' });
		};

		this.onInitError = function (e) {
			node.ready = false;
			node.status({ fill: 'red', shape: 'ring', text: 'Init error' });
			node.error('Kafka Producer Init error', e);
		};

		/**
		 * Serializes the message payload or key to a buffer.
		 * @param {any} msg The message to send to the broker.
		 * @param {string} toType The type to convert the message to.
		 * @returns {Buffer} The message as a buffer.
		 */
		function convertMessage(msg, toType) {
			if (Buffer.isBuffer(msg)) {
				return msg;
			}

			if (toType === 'json') {
				return Buffer.from(JSON.stringify(msg));
			} else if (toType === 'string') {
				return Buffer.from(msg.toString());
			}

			return msg;
		}

		this.init = async function () {
			node.producer = broker.server.producer({
				kafkaJS: {
					acks: acksDict[config.acknowledge],
					timeout: config.responsetimeout
				}
			});

			node.status({ fill: 'yellow', shape: 'ring', text: 'Initializing' });

			await node.producer.connect();

			this.onConnect();
		};

		this.init()
			.catch(e => this.onInitError(e));

		this.on('input', function (msg, send, done) {
			if (!node.ready || !msg.payload) return;

			if (!node.producer) {
				node.error('Kafka Producer not initialized');
			}

			try {
				let key = node.sendOptions.key || msg.key || null;
				if (key !== null && key !== undefined) key = convertMessage(key, config.keytype);

				let value = msg.payload;
				if (value !== null && value !== undefined) value = convertMessage(value, config.valuetype);

				const sendOptions = {
					topic: node.sendOptions.topic || msg.topic || null,
					messages: [
						{
							key,
							partition: node.sendOptions.partition || msg.partition || null,
							headers: Object.keys(node.sendOptions.headers).length === 0 ? msg.headers : node.sendOptions.headers,
							value,
						},
					],
				};

				node.producer
					.send(sendOptions)
					.then(() => {
						node.status({ fill: 'green', shape: 'dot', text: 'Sent' });

						send([msg, null]);
						done();
					})
					.catch((ex) => {
						node.status({ fill: 'red', shape: 'ring', text: 'Error' });

						send([null, { ...msg, error: ex }]);
						done(ex);
					});

				node.status({ fill: 'blue', shape: 'ring', text: 'Sending' });
			}
			catch (ex) {
				node.status({ fill: 'red', shape: 'ring', text: 'Error' });

				send([null, { ...msg, error: ex }]);
				done(ex);

			}
		});

		this.on('close', function (removed, done) {
			done = typeof done === 'function' ? done : typeof removed === 'function' ? removed : null;

			node.producer &&
				node.producer
					.disconnect()
					.then(() => {
						node.status({ fill: 'grey', shape: 'ring', text: 'Disconnected' });
						done();
					})
					.catch((ex) => {
						done(ex);
					});
		});
	}
	RED.nodes.registerType('bishop-kafka-producer', KafkaProducerNode);
};
