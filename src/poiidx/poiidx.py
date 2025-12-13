import psycopg
from typing import Optional


class POIIdex:
    VERSION = 1
    
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432
    ):
        """
        Initialize POIIdex with PostgreSQL database connection.
        
        Args:
            host: Database host address
            database: Database name
            user: Database user
            password: Database password
            port: Database port (default: 5432)
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection: Optional[psycopg.Connection] = None
        
        # Establish connection
        self._connect()
        
        # Initialize schema if needed
        self._initialize_schema()
        
        # Check version
        self._check_version()
    
    def _connect(self) -> None:
        """Establish connection to the PostgreSQL database."""
        try:
            self.connection = psycopg.connect(
                host=self.host,
                dbname=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
        except psycopg.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL database: {e}")
    
    def _initialize_schema(self) -> None:
        """Initialize the database schema if it doesn't exist."""
        if not self.connection:
            raise RuntimeError("No database connection available")
        
        with self.connection.cursor() as cursor:
            # Check if meta table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'meta'
                )
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                return  # Schema already initialized
            
            # Create meta table
            cursor.execute("""
                CREATE TABLE meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            # Insert initial version
            cursor.execute("""
                INSERT INTO meta (key, value) VALUES ('version', %s)
            """, (str(self.VERSION),))
            
            self.connection.commit()
    
    def _check_version(self) -> None:
        """Check if the database schema version matches the expected version."""
        if not self.connection:
            raise RuntimeError("No database connection available")
        
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT value FROM meta WHERE key = 'version'
            """)
            result = cursor.fetchone()
            
            if not result:
                raise RuntimeError("Version not found in meta table")
            
            db_version = int(result[0])
            if db_version != self.VERSION:
                raise RuntimeError(
                    f"Database schema version mismatch: "
                    f"expected {self.VERSION}, found {db_version}"
                )
    
    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None