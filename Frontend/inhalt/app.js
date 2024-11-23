const API_BASE_URL = 'http://localhost:3000'; // Backend-Endpunkt
//Für Nicht Lokale Endpunkte ändern

const cardList = document.getElementById('cardList');

// Renderkarten
const renderCards = (cards) => {
    cardList.innerHTML = '';
    if (cards.length === 0) {
        cardList.innerHTML = '<p>Keine Karten gefunden.</p>';
        return;
    }

    cards.forEach(card => {
        const li = document.createElement('li');
        li.classList.add('card-item');

        li.innerHTML = `
            <img src="${card.imageurl}" alt="${card.name}" class="card-image">
            <div class="card-content">
                <p class="card-title">${card.name}</p>
                <p class="card-info"><strong>Artist:</strong> ${card.artist || 'Unbekannt'}</p>
                <p class="card-info"><strong>Rarity:</strong> ${card.rarity || 'Keine Angabe'}</p>
                <p class="card-info"><strong>Subtypes:</strong> ${card.subtypes || 'Keine Angabe'}</p>
                <p class="card-info"><strong>Text:</strong> ${card.text || 'Keine Beschreibung'}</p>
            </div>
        `;
        cardList.appendChild(li);
    });
};

// Alle Karten abrufen
const fetchAllCards = async () => {
    try {
        const response = await fetch(`${API_BASE_URL}/cards`);
        if (!response.ok) throw new Error(`HTTP Error: ${response.status}`);
        const cards = await response.json();
        renderCards(cards);
    } catch (error) {
        console.error('Fehler beim Abrufen der Karten:', error);
    }
};

// Karte nach Name suchen
const fetchCardByName = async (name) => {
    try {
        const response = await fetch(`${API_BASE_URL}/cards/${encodeURIComponent(name)}`);
        if (!response.ok) throw new Error(`HTTP Error: ${response.status}`);
        const card = await response.json();
        renderCards(card);
    } catch (error) {
        console.error('Fehler beim Abrufen der Karte:', error);
    }
};

// Event-Listener
document.addEventListener('DOMContentLoaded', fetchAllCards);
document.getElementById('loadAllCards').addEventListener('click', fetchAllCards);
document.getElementById('searchCardButton').addEventListener('click', () => {
    const name = document.getElementById('searchCard').value.trim();
    if (name) {
        fetchCardByName(name);
    } else {
        alert('Bitte geben Sie einen Namen ein.');
    }
});
