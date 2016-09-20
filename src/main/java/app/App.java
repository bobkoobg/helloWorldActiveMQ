package app;

import java.util.Random;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class App {

    public static void main( String[] args ) throws Exception {
        thread( new HelloWorldProducer(), false, 1 );
        thread( new HelloWorldProducer(), false, 2 );
        
        thread( new HelloWorldConsumer(), false, 1 );
        thread( new HelloWorldConsumer(), false, 2 );
    }

    public static void thread( Runnable runnable, boolean daemon, int params ) {
        Thread brokerThread = new Thread( runnable );
        if ( params > 0 ) {
            if ( runnable instanceof HelloWorldConsumer ) {
                brokerThread = new Thread( new HelloWorldConsumer( params ) );
            }
        }
        brokerThread.setDaemon( daemon );
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {

        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory( "vm://localhost" );

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue( "BOBKOO.ROCKS" );

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer( destination );
                producer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );

                // Create a messages
                int isLooping = 0;

                while ( isLooping < 10 ) {
                    String text = "Hello world *" + isLooping + "*! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                    TextMessage message = session.createTextMessage( text );

                    // Tell the producer to send the message
                    System.out.println( "Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName() );
                    producer.send( message );

                    isLooping++;
                }

                // Clean up
                session.close();
                connection.close();

                System.out.println( "Sent everything, lets have some fun now!" );
            } catch ( Exception e ) {
                System.out.println( "Caught: " + e );
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {

        int uniqueId = 0;

        public HelloWorldConsumer( int params ) {
            uniqueId = params;
        }

        public HelloWorldConsumer() {
        }

        public void run() {
            try {

                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory( "vm://localhost" );

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener( this );

                // Create a Session
                Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue( "BOBKOO.ROCKS" );

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer( destination );

                Random rand = new Random();

                int sleeper = rand.nextInt( 5 ) + 1;
                System.out.println( "Hi, I am #" + uniqueId + " and I will sleep for " + sleeper * 1000 + " secs..." );
                Thread.sleep( sleeper * 1000 );

                // Wait for a message
                Message message;
                int isLooping = 0;

                while ( isLooping < 10 ) {
                    message = consumer.receive( 1000 );
                    if ( message == null ) {
                        System.out.println( "# " + uniqueId + " # says : No more, leave me alone now!" );
                        break;
                    }
                    if ( message instanceof TextMessage ) {
                        TextMessage textMessage = ( TextMessage ) message;
                        String text = textMessage.getText();
                        System.out.println( "# " + uniqueId + "# Received: " + text );
                    } else {
                        System.out.println( "# " + uniqueId + "# Received: " + message );
                    }

                    isLooping++;
                    System.out.println( "Sleeping!" );
                    Thread.sleep( 1500 );
                }

                consumer.close();
                session.close();
                connection.close();
            } catch ( Exception e ) {
                System.out.println( "Caught: " + e );
                e.printStackTrace();
            }
        }

        public synchronized void onException( JMSException ex ) {
            System.out.println( "JMS Exception occured.  Shutting down client." );
        }
    }

}
