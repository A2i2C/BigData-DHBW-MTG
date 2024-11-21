import express, { Request, Response } from 'express';
import cors from 'cors';
import mysql from 'mysql2/promise';

// Initialize Express App
const app = express();
const PORT = 5201;

// MySQL Connection Pool
const pool = mysql.createPool({
    user: 'root',
    port: 3306,   
    password: 'userpassword', 
    database: 'mtg_databse'
});

// Enable CORS
app.use(cors());

// Middleware
app.use(express.json());

// GET Request to Fetch Cards
app.get('/cards', async (req: Request, res: Response) => {
    try {
        const [rows] = await pool.query('SELECT * FROM mtg_cards');
        res.json(rows); // Sende die abgerufenen Karten als JSON zurück
    } catch (error) {
        console.error('Error fetching cards:', error);
        res.status(500).send('Internal Server Error');
    }
});

// Start the Server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
