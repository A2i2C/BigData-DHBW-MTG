import express, { Request, Response } from 'express';
import cors from 'cors';
import mysql from 'mysql2/promise';

// Initialize Express App
const app = express();
const PORT = process.env.PORT || 3000;

// MySQL Connection Pool
const pool = mysql.createPool({
    host: 'localhost', // Aktualisieren Sie dies bei Bedarf
    user: 'root', // Passen Sie den Benutzernamen an
    password: 'password', // Passen Sie das Passwort an
    database: 'database_name', // Setzen Sie den Datenbanknamen
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Enable CORS
app.use(cors());

// Middleware
app.use(express.json());

// GET Request to Fetch Cards
app.get('/cards', async (req: Request, res: Response) => {
    try {
        const [rows] = await pool.query('SELECT * FROM cards');
        res.json(rows);
    } catch (error) {
        console.error('Error fetching cards:', error);
        res.status(500).send('Internal Server Error');
    }
});