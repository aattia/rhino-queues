using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Queues.Tests.FromUsers
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Transactions;
    using Model;
    using Protocol;
    using Xunit;

    public class FromAlbert : IDisposable
    {
        private QueueManager _receiver;
        private bool _keepRunning = true;
        private readonly List<string> msgs = new List<string>();

        public void Dispose()
        {
            _receiver.Dispose();
        }

        public FromAlbert()
        {
            if (Directory.Exists("test3.esent"))
                Directory.Delete("test3.esent", true);

            using (var tx = new TransactionScope())
            {
                _receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 4545), "test3.esent");

                _receiver.CreateQueues("testPriority");
                tx.Complete();
            }
        }

        private void Receive(Object nothing)
        {
            while (_keepRunning)
            {
                using (var tx = new TransactionScope())
                {
                    Message msg;
                    try
                    {
                        msg = _receiver.Receive("testPriority", null, new TimeSpan(0, 0, 10));
                    }
                    catch (TimeoutException)
                    {
                        continue;
                    }
                    catch (ObjectDisposedException)
                    {
                        continue;
                    }
                    lock (msgs)
                    {
                        msgs.Add(Encoding.ASCII.GetString(msg.Data));
                        Console.WriteLine(msgs.Count);
                    }
                    tx.Complete();
                }
            }
        }

        public void SendDirectly(short priority)
        {

            using (var tx = new TransactionScope())
            {
                _receiver.EnqueueDirectlyTo("testPriority", null, new MessagePayload
                {
                    Data = Encoding.ASCII.GetBytes("Message " + priority),
                    Priority = priority

                });
                tx.Complete();
            }


        }

        [Fact]
        public void ReceiveMessagesFromQueueShouldRespectTheMessagePriority()
        {
            SendDirectly(10);
            SendDirectly(100);
            SendDirectly(5);

            ThreadPool.QueueUserWorkItem(Receive);

            while (true)
            {
                lock (msgs)
                {
                    if (msgs.Count > 2) break;    
                }
                Thread.Sleep(200);
            }
            _receiver.Dispose();
            _keepRunning = false;

            Assert.Equal(msgs[0],"Message 5");
            Assert.Equal(msgs[1], "Message 10");
            Assert.Equal(msgs[2], "Message 100");
            
        }

        [Fact]
        public void SendShouldThrowIfCalledWhenQueueManagerIsStartedWithoutAnIpEndpint()
        {
            using (var testManager = new QueueManager("test.esent"))
            {
                Assert.Throws(typeof(InvalidOperationException),
                         () => testManager.Send(new Uri("rhino.queues://localhost:4545/test"),
                                                new MessagePayload
                                                {
                                                    Data = Encoding.ASCII.GetBytes("Message")
                                                }
                                   ));     
            }
            
        }

        [Fact]
        public void WaitForAllMessagesToBeSentShouldThrowIfCalledWhenQueueManagerIsStartedWithoutAnIpEndpint()
        {
            using (var testManager = new QueueManager("test.esent"))
            {
                Assert.Throws(typeof(InvalidOperationException),
                         testManager.WaitForAllMessagesToBeSent);
            }
        }


    }
}
