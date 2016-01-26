package org.apache.nifi.jms.processors;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.nifi.processor.Processor;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

public class CommonTest {

    @Test
    public void validateServiceIsLocatableViaServiceLoader() {
        ServiceLoader<Processor> loader = ServiceLoader.<Processor> load(Processor.class);
        Iterator<Processor> iter = loader.iterator();
        boolean pubJmsPresent = false;
        boolean consumeJmsPresent = false;
        while (iter.hasNext()) {
            Processor p = iter.next();
            if (p.getClass().getSimpleName().equals(PublishJMS.class.getSimpleName())) {
                pubJmsPresent = true;
            } else if (p.getClass().getSimpleName().equals(ConsumeJMS.class.getSimpleName())) {
                consumeJmsPresent = true;
            }

        }
        assertTrue(pubJmsPresent);
        assertTrue(consumeJmsPresent);
    }

    static JmsTemplate buildJmsTemplateForDestination(String destinationName, boolean pubSub) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        CachingConnectionFactory cf = new CachingConnectionFactory(connectionFactory);

        JmsTemplate jmsTemplate = new JmsTemplate(cf);
        jmsTemplate.setDefaultDestinationName(destinationName);
        jmsTemplate.setPubSubDomain(pubSub);
        return jmsTemplate;
    }
}
