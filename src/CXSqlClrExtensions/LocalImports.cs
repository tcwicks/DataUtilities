using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChillX.Core.Structures
{
    public class ThreadsafeCounterLong
    {
        /// <summary>
        /// Thread safe integer value wrapper with counter increment and decrement support
        /// Value is initialized to ResetValue parameter.
        /// </summary>
        /// <param name="_minValue">Minimum value below which on increment of the counter <see cref="NextID"/> the value is reset to the specified reset value. Default is long.MinValue+1</param>
        /// <param name="_maxValue">Maximum value above which on decrement of the counter <see cref="PreviousID"/> the value is reset to the specified reset value. Default is long.MaxValue-1</param>
        /// <param name="_resetValue">Reset value to which the counter value is reset when it cross the boundaries set by <see cref="MinValue"/> and <see cref="MaxValue"/> </param>
        /// <exception cref="ArgumentException"></exception>
        public ThreadsafeCounterLong(long _minValue = long.MinValue + 1, long _maxValue = long.MaxValue - 1, long _resetValue = 0)
        {
            if (_maxValue <= MinValue) { throw new ArgumentException(@"_maxValue must be greater than _minValue"); }
            m_MinValue = _minValue;
            m_MaxValue = _maxValue;
            m_ResetValue = _resetValue;
            _value = m_ResetValue;
        }
        private readonly object SyncLock = new object();
        private long m_MinValue = 1;
        public long MinValue { get { return m_MinValue; } }
        private long m_MaxValue = 100000;
        public long MaxValue { get { return m_MaxValue; } }
        private long m_ResetValue = 0;
        public long ResetValue { get { return m_ResetValue; } }

        private long _value = 0;

        /// <summary>
        /// Current value of counter
        /// Note: this is using a read of a volatile int and is therefore protected by a memory barrier for read ordering
        /// Also see <see cref="ValueInterlocked"/>
        /// </summary>
        public long Value
        {
            get { return Volatile.Read(ref _value); }
            set
            {
                Interlocked.Exchange(ref _value, value);
            }
        }

        /// <summary>
        /// Current value of counter
        /// Note: value is retrieved via Interloked.CompareExchange and is therefore protected against both read and write ordering
        /// Also see <see cref="Value"/> which is faster for reads as it is implemented using a simple volatile read.
        /// </summary>
        public long ValueInterlocked
        {
            get { return Interlocked.CompareExchange(ref _value, 0, 0); }
            set { Interlocked.Exchange(ref _value, value); }
        }

        /// <summary>
        /// Increment counter by one.
        /// If value exceeds <see cref="MaxValue"/> it is rest to <see cref="ResetValue"/> and then incremented by one
        /// </summary>
        /// <returns>Value of counter</returns>
        public virtual long NextID()
        {
            long result = Interlocked.Increment(ref _value);
            if (result > m_MaxValue)
            {
                lock (SyncLock)
                {
                    result = _value;
                    if (result > m_MaxValue)
                    {
                        Interlocked.Exchange(ref _value, m_ResetValue);
                    }
                }
                result = Interlocked.Increment(ref _value);
            }
            return result;
        }

        /// <summary>
        /// Increment counter by specified value
        /// </summary>
        /// <param name="incrementBy">Value to increment counter by. Note: Value must be positive</param>
        /// <returns>New value after incrementing counter</returns>
        public virtual long Increment(long incrementBy)
        {
            if (incrementBy < 0)
            {
                return ValueInterlocked;
            }
            long result;
            long newValue;
            result = Interlocked.Increment(ref _value);
            incrementBy -= 1;
            newValue = result + incrementBy;
            while (incrementBy > 0)
            {
                if (Interlocked.CompareExchange(ref _value, newValue, result) == result)
                {
                    break;
                }
                result = Interlocked.Increment(ref _value);
                incrementBy -= 1;
                newValue = result + incrementBy;
            }
            return newValue;
        }

        /// <summary>
        /// Decrement counter by one.
        /// If value drops below <see cref="MinValue"/> it is reset to <see cref="ResetValue"/> and then decremented by one
        /// </summary>
        /// <returns>Value of counter</returns>
        public virtual long PreviousID()
        {
            long result = Interlocked.Decrement(ref _value);
            if (result < m_MinValue)
            {
                lock (SyncLock)
                {
                    result = _value;
                    if (result < m_MinValue)
                    {
                        Interlocked.Exchange(ref _value, m_ResetValue);
                    }
                }
                result = Interlocked.Decrement(ref _value);
            }
            return result;
        }

        /// <summary>
        /// Decrement counter by specified value
        /// </summary>
        /// <param name="decrementBy">Value to decrement counter by. Note: Value must be negative</param>
        /// <returns></returns>
        public virtual long Decrement(long decrementBy)
        {
            if (decrementBy > 0)
            {
                return ValueInterlocked;
            }
            long result;
            long newValue;
            result = Interlocked.Decrement(ref _value);
            decrementBy += 1;
            newValue = result + decrementBy;
            while (decrementBy < 0)
            {
                if (Interlocked.CompareExchange(ref _value, newValue, result) == result)
                {
                    break;
                }
                result = Interlocked.Increment(ref _value);
                decrementBy += 1;
                newValue = result + decrementBy;
            }
            return newValue;
        }

    }

    public class ChannelisedQueueManager<T> : IDisposable
    {
        private ReaderWriterLockSlim m_Lock = new ReaderWriterLockSlim();
        Dictionary<object, ThreadSafeQueue<T>> m_QueueDict = new Dictionary<object, ThreadSafeQueue<T>>();
        public ThreadSafeQueue<T> GetQueue(object Channel)
        {
            ThreadSafeQueue<T> QueueInstance = null;
            bool FoundQueue = false;
            m_Lock.EnterReadLock();
            try
            {
                FoundQueue = m_QueueDict.TryGetValue(Channel, out QueueInstance);
            }
            finally
            {
                m_Lock.ExitReadLock();
            }
            if (!FoundQueue)
            {
                m_Lock.EnterWriteLock();
                try
                {
                    FoundQueue = m_QueueDict.TryGetValue(Channel, out QueueInstance);
                    if (!FoundQueue)
                    {
                        QueueInstance = new ThreadSafeQueue<T>();
                    }
                    m_QueueDict.Add(Channel, QueueInstance);
                }
                finally
                {
                    m_Lock.ExitWriteLock();
                }
            }
            return QueueInstance;
        }
        public void GetChannels(List<object> ChannelListToPopulate)
        {
            m_Lock.EnterReadLock();
            try
            {
                ChannelListToPopulate.AddRange(m_QueueDict.Keys);
            }
            finally
            {
                m_Lock.ExitReadLock();
            }
        }

        private bool m_IsDisposed = false;
        public void Dispose()
        {
            if (!m_IsDisposed)
            {
                m_IsDisposed = true;
                DoDispose(true);
            }
        }
        private void DoDispose(bool isDisposing)
        {
            if (isDisposing)
            {
                m_Lock.Dispose();
                m_Lock = null;
                foreach (KeyValuePair<object, ThreadSafeQueue<T>> Pair in m_QueueDict)
                {
                    Pair.Value.Clear();
                    Pair.Value.Dispose();
                }
                m_QueueDict.Clear();
            }
        }
    }
    public class ThreadSafeQueue<T> : IDisposable
    {
        private Queue<T> m_Queue = new Queue<T>();
        private ReaderWriterLockSlim m_Lock = new ReaderWriterLockSlim();
        public ThreadSafeQueue()
        {
        }

        public void Enqueue(T item)
        {
            m_Lock.EnterWriteLock();
            try
            {
                m_Queue.Enqueue(item);
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
        }
        public bool EnqueueCapped(T item, int maxSize)
        {
            m_Lock.EnterWriteLock();
            try
            {
                if (m_Queue.Count > maxSize) { return false; }
                m_Queue.Enqueue(item);
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
            return true;
        }

        public void Clear()
        {
            m_Lock.EnterWriteLock();
            try
            {
                m_Queue.Clear();
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
        }
        public int Count
        {
            get
            {
                m_Lock.EnterReadLock();
                try
                {
                    return m_Queue.Count;
                }
                finally
                {
                    m_Lock.ExitReadLock();
                }
            }
        }

        public bool HasItems()
        {
            m_Lock.EnterReadLock();
            try
            {
                return m_Queue.Count > 0;
            }
            finally
            {
                m_Lock.ExitReadLock();
            }
        }
        public bool IsEmpty()
        {
            m_Lock.EnterReadLock();
            try
            {
                return m_Queue.Count == 0;
            }
            finally
            {
                m_Lock.ExitReadLock();
            }
        }
        public T GetDefault()
        {
            return default(T);
        }

        public T DeQueue(out bool Success)
        {
            Success = true;
            m_Lock.EnterWriteLock();
            try
            {
                if (m_Queue.Count > 0)
                {
                    return m_Queue.Dequeue();
                }
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
            Success = false;
            return default(T);
        }

        public int DeQueue(int requestedCount, Queue<T> destinationQueue, out bool success, bool requestedCountIsMandatory = false)
        {
            success = false;
            int counter = 0;
            m_Lock.EnterUpgradeableReadLock();
            try
            {
                if (requestedCountIsMandatory && (m_Queue.Count < requestedCount))
                {
                    return 0;
                }
                m_Lock.EnterWriteLock();
                try
                {

                    while (m_Queue.Count > 0 && destinationQueue.Count < requestedCount)
                    {
                        counter++;
                        destinationQueue.Enqueue(m_Queue.Dequeue());
                        success = true;
                    }
                }
                finally
                {
                    m_Lock.ExitWriteLock();
                }
            }
            finally
            {
                m_Lock.ExitUpgradeableReadLock();
            }

            return counter;
        }

        public int DeQueue(int requestedCount, IList<T> destinationList, out bool success, bool requestedCountIsMandatory = false)
        {
            success = false;
            int counter = 0;
            m_Lock.EnterUpgradeableReadLock();
            try
            {
                if (requestedCountIsMandatory && (m_Queue.Count < requestedCount))
                {
                    return 0;
                }
                m_Lock.EnterWriteLock();
                try
                {

                    while (m_Queue.Count > 0 && destinationList.Count < requestedCount)
                    {
                        counter++;
                        destinationList.Add(m_Queue.Dequeue());
                        success = true;
                    }
                }
                finally
                {
                    m_Lock.ExitWriteLock();
                }
            }
            finally
            {
                m_Lock.ExitUpgradeableReadLock();
            }

            return counter;
        }
        public int DeQueue(int requestedCount, ThreadSafeQueue<T> destinationQueue, out bool success, bool requestedCountIsMandatory = false)
        {
            success = false;
            int counter = 0;
            m_Lock.EnterUpgradeableReadLock();
            try
            {
                if (requestedCountIsMandatory && (m_Queue.Count < requestedCount))
                {
                    return 0;
                }
                m_Lock.EnterWriteLock();
                try
                {
                    while (m_Queue.Count > 0 && destinationQueue.Count < requestedCount)
                    {
                        counter++;
                        destinationQueue.Enqueue(m_Queue.Dequeue());
                        success = true;
                    }
                }
                finally
                {
                    m_Lock.ExitWriteLock();
                }
            }
            finally
            {
                m_Lock.ExitUpgradeableReadLock();
            }
            return counter;
        }

        public int DeQueue(ThreadSafeQueue<T> destinationQueue)
        {
            int counter = 0;
            m_Lock.EnterWriteLock();
            try
            {
                while (m_Queue.Count > 0)
                {
                    counter++;
                    destinationQueue.Enqueue(m_Queue.Dequeue());
                }
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
            return counter;
        }

        public int DeQueue(Queue<T> destinationQueue)
        {
            int counter = 0;
            m_Lock.EnterWriteLock();
            try
            {
                counter = m_Queue.Count;
                for (int I = 0; I < counter; I++)
                {
                    destinationQueue.Enqueue(m_Queue.Dequeue());
                }
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
            return counter;
        }
        public int DeQueue(IList<T> destinationList)
        {
            int counter = 0;
            m_Lock.EnterWriteLock();
            try
            {
                counter = m_Queue.Count;
                for (int I = 0; I < counter; I++)
                {
                    destinationList.Add(m_Queue.Dequeue());
                }
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
            return counter;
        }

        public T DeQueue()
        {
            m_Lock.EnterWriteLock();
            try
            {
                if (m_Queue.Count > 0)
                {
                    return m_Queue.Dequeue();
                }
            }
            finally
            {
                m_Lock.ExitWriteLock();
            }
            return default(T);
        }

        private bool m_IsDisposed = false;
        public void Dispose()
        {
            if (!m_IsDisposed)
            {
                m_IsDisposed = true;
                DoDispose(true);
            }
        }
        private void DoDispose(bool isDisposing)
        {
            if (isDisposing)
            {
                m_Lock.Dispose();
                m_Lock = null;
            }
        }
    }

}
