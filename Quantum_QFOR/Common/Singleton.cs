namespace Quantum_QFOR
{
    ////Thread-safe (Double-checked Locking) initialization of singleton
    public class Singleton
    {
        // .NET guarantees thread safety for static initialization
        private static Singleton instance = null;
        private string Name { get; set; }
        private string IP { get; set; }
        private Singleton()
        {
        }
        // Lock synchronization object
        private static object syncLock = new object();

        public static Singleton Instance
        {
            get
            {
                lock (syncLock)
                {
                    if (Singleton.instance == null)
                        Singleton.instance = new Singleton();

                    return Singleton.instance;
                }
            }
        }

        public SessionValue GetAll()
        {
            SessionValue session = new SessionValue();
            return session;
        }

    }
}