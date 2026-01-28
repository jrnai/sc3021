import clashroyale
import pandas as pd
import time
import os

# --- 1. SETUP ---
MY_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImI0MmI5NmM4LWRhZTItNDIzMi1hNTZjLWE1NGFkMmFiZDIzYSIsImlhdCI6MTc2OTU4MjkzNCwic3ViIjoiZGV2ZWxvcGVyL2JjNDM0NTNlLTEzNzAtMzViYi0yZjVkLTk5ZDJhYzYxMGJlZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyIxNTUuNjkuMTkzLjY2Il0sInR5cGUiOiJjbGllbnQifV19.oCS7HZKNE-tE8rVbSuRXTcqmQrcSgR4KIkf1uCL6nKumGduh4n3UGsTPqie5WIuywS3ePfn8qxneoMM1n9kewA"

# Changed timeout to 10s to fail faster if internet hangs
client = clashroyale.OfficialAPI(MY_TOKEN, timeout=10)
TARGET_BATTLES = 50000 
FILENAME = "battles_master.csv"

# Queue of players to scan.
player_queue = ["#V92CQYQPJ", "#2U298Y", "#R98982", "#9LPRGP9", "#29U09V8J"] 
scanned_players = set()
unique_battle_ids = set()

# --- 2. RARITY MAP ---
print("Building Rarity Map...")
try:
    all_cards_raw = client.get_all_cards()
    RARITY_MAP = {card.name: card.rarity for card in all_cards_raw}
    print(f"✅ Map Built. {len(RARITY_MAP)} cards found.")
except Exception as e:
    print(f"❌ Error connecting to API: {e}")
    exit()

def get_real_level(card_name, api_level):
    rarity = RARITY_MAP.get(card_name, "Common").title()
    if rarity == "Legendary": return api_level + 8
    if rarity == "Epic":      return api_level + 5
    if rarity == "Rare":      return api_level + 2
    if rarity == "Champion":  return api_level + 10
    return api_level 

# --- 3. THE SPIDER LOOP ---
battles_collected = 0

print(f"\n--- STARTING SPIDER CRAWL ---")
print(f"Goal: {TARGET_BATTLES} Battles")
print("Saving to: " + FILENAME)
print("Press Ctrl+C to stop safely at any time.\n")

while battles_collected < TARGET_BATTLES and len(player_queue) > 0:
    current_player = player_queue.pop(0)
    
    if current_player in scanned_players:
        continue
    scanned_players.add(current_player)
    
    try:
        battles = client.get_player_battles(current_player)
        new_rows = []
        
        for battle in battles:
            # 1. Get Tags
            tag_a = battle.team[0].tag
            tag_b = battle.opponent[0].tag

            # 2. Sort Tags (The Fix for Duplicates)
            sorted_tags = sorted([tag_a, tag_b])
            b_id = f"{battle.battle_time}_{sorted_tags[0]}_{sorted_tags[1]}"

            # 3. Check ONCE (The Fix for your Bug)
            if b_id in unique_battle_ids or battle.type != 'PvP':
                continue
            
            unique_battle_ids.add(b_id)
            
            # --- DATA EXTRACTION ---
            p1 = battle.team[0]
            p2 = battle.opponent[0]
            
            if p1.crowns > p2.crowns: outcome = 1
            elif p1.crowns < p2.crowns: outcome = 0
            else: outcome = 0.5
            
            row = {
                'battle_time': battle.battle_time,
                'game_mode': battle.game_mode.name,
                'p1_tag': p1.tag,
                'p2_tag': p2.tag,
                'p1_trophies': getattr(p1, 'starting_trophies', None),
                'p2_trophies': getattr(p2, 'starting_trophies', None),
                'outcome': outcome
            }
            
            for i, card in enumerate(p1.cards):
                row[f'p1_card_{i+1}'] = card.name
                row[f'p1_card_{i+1}_lvl'] = get_real_level(card.name, card.level)
                
            for i, card in enumerate(p2.cards):
                row[f'p2_card_{i+1}'] = card.name
                row[f'p2_card_{i+1}_lvl'] = get_real_level(card.name, card.level)
                
            new_rows.append(row)
            
            if p2.tag not in scanned_players:
                player_queue.append(p2.tag)
        
        # --- SAVE BATCH ---
        if new_rows:
            df = pd.DataFrame(new_rows)
            header = not os.path.exists(FILENAME)
            df.to_csv(FILENAME, mode='a', header=header, index=False)
            
            battles_collected += len(new_rows)
            print(f"Collected: {battles_collected}/{TARGET_BATTLES} | Unique Players: {len(scanned_players)} | Queue: {len(player_queue)}")
            
    except Exception as e:
        print(f"Skipping {current_player}: {e}")
        time.sleep(0.5) 

print("\n✅ DONE! Data collection finished.")