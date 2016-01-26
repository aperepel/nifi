/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.jms.processors;

import java.util.Map;
import java.util.Map.Entry;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.nifi.logging.ProcessorLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsHeaders;

/**
 * Generic publisher of messages to JMS compliant messaging system.
 */
final class JMSPublisher extends JMSWorker {

    private final static Logger logger = LoggerFactory.getLogger(JMSPublisher.class);


    /**
     * Creates an instance of this publisher
     *
     * @param jmsTemplate
     *            instance of {@link JmsTemplate}
     * @param processLog
     *            instance of {@link ProcessorLog}
     */
    JMSPublisher(JmsTemplate jmsTemplate, ProcessorLog processLog) {
        super(jmsTemplate, processLog);
        if (logger.isInfoEnabled()) {
            logger.info("Created Message Publisher for '" + jmsTemplate.toString() + "'.");
        }
    }

    /**
     *
     * @param messageBytes
     */
    void publish(final byte[] messageBytes) {
        this.publish(messageBytes, null);
    }

    /**
     *
     * @param messageBytes
     * @param flowFileAttributes
     */
    void publish(final byte[] messageBytes, final Map<String, String> flowFileAttributes) {
        this.jmsTemplate.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(messageBytes);
                if (flowFileAttributes != null && !flowFileAttributes.isEmpty()) {
                    if (flowFileAttributes.containsKey(JmsHeaders.DELIVERY_MODE)) {
                        message.setJMSDeliveryMode(Integer.parseInt(flowFileAttributes.get(JmsHeaders.DELIVERY_MODE)));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.EXPIRATION)) {
                        message.setJMSExpiration(Long.parseLong(flowFileAttributes.get(JmsHeaders.EXPIRATION)));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.PRIORITY)) {
                        message.setJMSPriority(Integer.parseInt(flowFileAttributes.get(JmsHeaders.PRIORITY)));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.REDELIVERED)) {
                        message.setJMSRedelivered(Boolean.parseBoolean(flowFileAttributes.get(JmsHeaders.REDELIVERED)));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.TIMESTAMP)) {
                        message.setJMSTimestamp(Long.parseLong(flowFileAttributes.get(JmsHeaders.TIMESTAMP)));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.CORRELATION_ID)) {
                        message.setJMSCorrelationID(flowFileAttributes.get(JmsHeaders.CORRELATION_ID));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.MESSAGE_ID)) {
                        message.setJMSMessageID(flowFileAttributes.get(JmsHeaders.MESSAGE_ID));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.TYPE)) {
                        message.setJMSType(flowFileAttributes.get(JmsHeaders.TYPE));
                    } else if (flowFileAttributes.containsKey(JmsHeaders.REPLY_TO)) {
                        Destination destination = buildDestination(flowFileAttributes.get(JmsHeaders.REPLY_TO));
                        if (destination != null) {
                            message.setJMSReplyTo(destination);
                        } else {
                            logUnbuildableDestination(flowFileAttributes.get(JmsHeaders.REPLY_TO), JmsHeaders.REPLY_TO);
                        }
                    } else if (flowFileAttributes.containsKey(JmsHeaders.DESTINATION)) {
                        Destination destination = buildDestination(flowFileAttributes.get(JmsHeaders.DESTINATION));
                        if (destination != null) {
                            message.setJMSDestination(destination);
                        } else {
                            logUnbuildableDestination(flowFileAttributes.get(JmsHeaders.DESTINATION),
                                    JmsHeaders.DESTINATION);
                        }
                    }
                    // set message properties
                    for (Entry<String, String> entry : flowFileAttributes.entrySet()) {
                        if (!entry.getKey().startsWith(JmsHeaders.PREFIX)) {
                            message.setStringProperty(entry.getKey(), entry.getValue());
                        }
                    }
                }
                return message;
            }
        });
    }

    /**
     *
     */
    private void logUnbuildableDestination(String destinationName, String headerName) {
        logger.warn("Failed to determine destination type from destination name '" + destinationName + "'. The '"
                + headerName + "' will not be set.");
        processLog.warn("Failed to determine destination type from destination name '" + destinationName + "'. The '"
                + headerName + "' will not be set.");
    }

    /**
     *
     */
    private Destination buildDestination(final String destinationName) {
        Destination destination;
        if (destinationName.toLowerCase().contains("topic")) {
            destination = this.jmsTemplate.execute(new SessionCallback<Topic>() {
                @Override
                public Topic doInJms(Session session) throws JMSException {
                    return session.createTopic(destinationName);
                }
            });
        } else if (destinationName.toLowerCase().contains("queue")) {
            destination = this.jmsTemplate.execute(new SessionCallback<Queue>() {
                @Override
                public Queue doInJms(Session session) throws JMSException {
                    return session.createQueue(destinationName);
                }
            });
        } else {
            destination = null;
        }
        return destination;
    }
}
