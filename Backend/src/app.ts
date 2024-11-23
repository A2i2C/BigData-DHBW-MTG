import express, { Request, Response } from 'express';
import cors from 'cors';
import { Pool } from 'pg';

// Initialize Express App
const app = express();
const PORT = 5201;

// PostgreSQL Connection Pool
const pool = new Pool({
    host: 'postgresql', // Update as necessary
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: 'postgres'
});

// Enable CORS
app.use(cors());
// Middleware
app.use(express.json());

// GET Request to Fetch all Cards
app.get('/cards', async (req: Request, res: Response) => {
    try {
        const result = await pool.query('SELECT * FROM cards LIMIT 102');
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching cards:', error);
        res.status(500).send('Internal Server Error');
    }
});

// GET Request to Fetch a Card by Name
app.get('/cards/:name', async (req: Request, res: Response) => {
    const { name } = req.params;
    try {
        const result = await pool.query('SELECT * FROM cards WHERE name ILIKE $1 LIMIT 102', [`%${name}%`]);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching card:', error);
        res.status(500).send('Internal Server Error');
    }
});