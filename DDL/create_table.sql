CREATE TABLE IF NOT EXISTS mtg_cards (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    artist VARCHAR(255),
    type VARCHAR(255),
    manaCost VARCHAR(255),
    rarity VARCHAR(255)
);