/** @import { NodeAPI } from 'node-red' */
/** @typedef { import('@confluentinc/kafka-javascript/types/kafkajs.d.ts').Consumer } Consumer */
/** @import { KafkaBrokerNode } from './broker' */

/**
 * @param {NodeAPI} RED Root node to register the components
 */
module.exports = function (RED) {
	function KafkaConsumerNode(config) {
		const node = this;

		RED.nodes.createNode(this, config);

		/** @type {Consumer} */
		this.consumer = null;

		/** @type {KafkaBrokerNode} */
		const broker = RED.nodes.getNode(config.broker);
		if (!broker) {
			node.status({ fill: 'red', shape: 'ring', text: 'Broker is missing.' });
			return;
		}

		/**
		 * @typedef {object} LastCommitted
		 * @property {number} partition
		 * @property {string} offset
		 * @property {string} topic
		 */
		/** @type {LastCommitted | null} */
		this.lastCommitted = null;

		const consumerOptions = {
			/** @type {string} */
			groupId: config.groupid,
		};

		const subscribeOptions = {
			/** @type {string} */
			topic: config.topic,
		};

		/**
		 * Deserializes the message payload or key to a buffer.
		 * @param {Buffer} msg The message received from the broker.
		 * @param {string} toType The type inside the Buffer to convert the message from.
		 * @returns {object | string | number | bigint} The message as the requested type.
		 */
		function convertMessage(msg, toType) {
			if (!Buffer.isBuffer(msg)) {
				return msg;
			}

			try {
				if (toType === 'json') {
					return JSON.parse(msg.toString());
				} else if (toType === 'string') {
					return msg.toString();
				}
			} catch (ex) {
				node.error('Kafka Consumer Error (message conversion)', ex);
			}

			return msg;
		}

		const runOptions = {
			eachMessage: async ({ topic, partition, message }) => {
				try {
					const headers = message.headers
						? Object.keys(message.headers).length === 0 ? null : {}
						: null;

					const msg = {
						payload: {
							key: convertMessage(message.key, config.keytype),
							value: convertMessage(message.value, config.valuetype),
							headers
						},
						bishop: {
							kafka: {
								topic,
								partition,
								offset: message.offset,
							}
						}
					};

					if (headers) {
						for (const [key, value] of Object.entries(message.headers)) {
							msg.payload.headers[key] = value.toString();
						}
					}

					node.send([msg, null]);
					node.status({ fill: 'green', shape: 'dot', text: 'Message received' });
				} catch (ex) {
					node.status({ fill: 'red', shape: 'ring', text: 'Error' });
					send([null, { ...msg, error: ex }]);
				}
			},
		};

		if (config.advancedoptions) {
			consumerOptions.sessionTimeout = config.sessiontimeout;
			consumerOptions.rebalanceTimeout = config.rebalancetimeout;
			consumerOptions.heartbeatInterval = config.heartbeatinterval;
			consumerOptions.metadataMaxAge = config.metadatamaxage;
			consumerOptions.maxBytesPerPartition = config.maxbytesperpartition;
			consumerOptions.minBytes = config.minbytes;
			consumerOptions.maxBytes = config.maxbytes;
			consumerOptions.maxWaitTimeInMs = config.maxwaittimeinms;

			consumerOptions.fromBeginning = config.frombeginning;

			consumerOptions.autoCommit = config.autocommit;
			consumerOptions.autoCommitInterval = config.autocommitinterval;
		}

		this.onConnect = function () {
			node.status({ fill: 'green', shape: 'ring', text: 'Ready' });
		};


		this.onInitError = function (e) {
			node.status({ fill: 'red', shape: 'ring', text: 'Init error' });
			node.error('Kafka Consumer Init error', e.toString());
		};

		this.init = async function init() {
			node.consumer = broker.server.consumer({
				kafkaJS: consumerOptions,
			});
			node.status({ fill: 'yellow', shape: 'ring', text: 'Initializing' });
			
			await node.consumer.connect();
			this.onConnect();

			await node.consumer.subscribe(subscribeOptions);

			await node.consumer.run(runOptions);

		};

		this.init()
			.catch((e) => this.onInitError(e));

		this.onPause = function (done) {
			node.consumer.pause([{ topic: subscribeOptions.topic }]);
			node.status({ fill: 'yellow', shape: 'ring', text: 'Paused' });
			done();
		}

		this.onResume = function (done) {
			node.consumer.resume([{ topic: subscribeOptions.topic }]);
			node.status({ fill: 'green', shape: 'ring', text: 'Resumed' });
			done();
		}

		this.onSeek = function(msg, done) {
			const offset = msg.payload.offset;
			const partition = msg.payload.partition;

			if (offset === null || offset === undefined || isNaN(offset)) {
				node.error("Seek error: invalid offset");
				done();
				return;
			}

			if (partition === null || partition === undefined || isNaN(partition)) {
				node.error("Seek error: invalid partition");
				done();
				return;
			}

			node.consumer.seek({
				topic: subscribeOptions.topic,
				partition,
				offset: offset.toString()
			});

			node.status({ fill: 'green', shape: 'ring', text: 'Seeked' });
			done();
		}

		this.onCommit = function(msg, done) {
			const partition = msg.payload.partition;
			const offset = msg.payload.offset;
			const topic = msg.payload.topic || subscribeOptions.topic;

			if (isNaN(partition)) {
				node.error("Commit error: invalid partition");
				done();
				return;
			}

			if (!topic) {
				node.error("Commit error: invalid topic");
				done();
				return;
			}

			if (isNaN(offset)) {
				node.error("Commit error: invalid offset");
				done();
				return;
			}

			if (
				this.lastCommitted &&
				this.lastCommitted.topic === topic &&
				this.lastCommitted.partition === partition &&
				parseInt(this.lastCommitted.offset) > parseInt(offset)
			) {
				node.warn(
					`Commit skipped: last committed offset (${this.lastCommitted.offset - 1}) for topic "${topic}" partition ${partition} 
					is greater than the offset (${offset}) being committed.`
				);
				done();
				return;
			}

			const commit = {
				partition,
				topic,
				offset: (parseInt(offset) + 1).toString()
			};

			node.consumer.commitOffsets([commit]);

			this.lastCommitted = commit;

			done();
		}

		node.on('input', function (msg, send, done) {
			switch(msg.payload.event) {
				case 'pause':
					this.onPause(done);
					break;
				case 'resume':
					this.onResume(done);
					break;
				case 'seek':
					this.onSeek(msg, done);
					break;
				case 'commit':
					this.onCommit(msg, done);
					break;
			}
		});

		node.on('close', function (done) {
			node.consumer
				.disconnect()
				.then(() => {
					node.status({ fill: 'grey', shape: 'ring', text: 'Disconnected' });
					done();
				})
				.catch((ex) => done(ex));
		});
	}
	RED.nodes.registerType('bishop-kafka-consumer', KafkaConsumerNode);
};
