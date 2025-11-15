# In[0]: Imports
from nba_api.stats.endpoints import teamdashlineups
from nba_api.stats.static import teams
import pandas as pd
import time
import os

# In[1]: Get Team Information
nba_teams = teams.get_teams()

# In[2]: Create Team Dictionary
team_dict = {}
for i in nba_teams:
    team_name = i['full_name']
    team_id = i['id']
    team_dict[team_name]= team_id

# In[3]: Define Function to Get Lineups with Measure Type and Season Type
def get_lineups(team_id_i, measure_type="Base", season_type="Regular Season", season="2024-25", retries=3, delay=2):
    """
    Fetches lineup data for a specific team, measure type, season type, and season from the NBA API.

    Args:
        team_id_i (int): The ID of the team.
        measure_type (str): The type of stats to fetch (e.g., "Base", "Advanced").
        season_type (str): The type of season ("Regular Season" or "Playoffs").
        season (str): The season identifier (e.g., "2024-25").
        retries (int): Number of retry attempts in case of API errors.
        delay (int): Delay in seconds between retries.

    Returns:
        pandas.DataFrame or None: DataFrame containing lineup data or None if fetching fails.
    """
    import time # Ensure time is available

    print(f"    Fetching {season_type} - {measure_type} stats for team ID {team_id_i} (Season: {season})...") # More specific print

    for attempt in range(retries):
        try:
            lineup = teamdashlineups.TeamDashLineups(
                date_from_nullable = "",
                date_to_nullable = "",
                game_id_nullable = "",
                game_segment_nullable = "",
                group_quantity = 5,
                last_n_games = 0,
                league_id_nullable = "00",
                location_nullable = "",
                measure_type_detailed_defense = measure_type, # Use the function argument
                month = 0,
                opponent_team_id = 0,
                outcome_nullable = "",
                pace_adjust = "N",
                plus_minus = "N",
                po_round_nullable = "",
                per_mode_detailed = "Totals",
                period = 0,
                rank = "N",
                season = season, # Use season parameter
                season_segment_nullable = "",
                season_type_all_star = season_type, # Use season_type parameter
                shot_clock_range_nullable = "",
                team_id = team_id_i,
                vs_conference_nullable = "",
                vs_division_nullable = "",
                timeout= 60
            )

            df_list = lineup.get_data_frames()

            # The lineup data is typically the second DataFrame (index 1)
            if df_list and len(df_list) > 1:
                all_lineups = df_list[1]
                # Add the season type column right after fetching
                all_lineups['SEASON_TYPE'] = season_type
                print(f"      Successfully fetched {len(all_lineups)} rows.")
                return all_lineups
            else:
                 print(f"      Warning: Unexpected data structure returned. DataFrames received: {len(df_list) if df_list else 'None'}")
                 return None # Return None if structure is wrong

        except Exception as e:
            print(f"      Attempt {attempt + 1} failed. Error: {str(e)}")
            if attempt < retries - 1:  # if not last attempt
                print(f"      Retrying in {delay} seconds...")
                time.sleep(delay)  # wait before retrying
            else:
                print(f"      Failed to get data after {retries} attempts.")
                return None # Return None after all retries fail

# In[4]: Main Loop to Fetch, Merge, and Aggregate Data for Both Season Types
league_lineup = pd.DataFrame()
# Define the season types you want to fetch
season_types_to_fetch = ["Regular Season", "Playoffs"]
# Define the season year you are interested in (can be overridden via NBA_SEASON env var)
target_season = os.getenv("NBA_SEASON", "2025-26")

print(f"Starting data fetch for season: {target_season}")
print("-" * 30)

for team_name, team_id_i in team_dict.items():
    print(f"\nProcessing team: {team_name} ({team_id_i})")

    team_has_data = False # Flag to track if any data was added for this team

    for season_type in season_types_to_fetch:
        print(f"  Processing {season_type} data...")

        # Get Base stats for the current season type
        team_lineup_base = get_lineups(team_id_i,
                                       measure_type="Base",
                                       season_type=season_type,
                                       season=target_season)
        time.sleep(0.6) # API Rate limit consideration

        # Get Advanced stats for the current season type
        team_lineup_advanced = get_lineups(team_id_i,
                                           measure_type="Advanced",
                                           season_type=season_type,
                                           season=target_season)

        # --- Merging Logic ---
        merged_data_for_season_type = None # To hold the result for this season type

        if team_lineup_base is not None and not team_lineup_base.empty and \
           team_lineup_advanced is not None and not team_lineup_advanced.empty:

            print(f"    Merging Base and Advanced {season_type} stats...")
            # Columns to keep from advanced (GROUP_ID + unique advanced stats)
            base_cols = set(team_lineup_base.columns)
            adv_cols = set(team_lineup_advanced.columns)
            # Exclude SEASON_TYPE from base_cols for comparison as it's added in both
            base_cols_for_compare = base_cols - {'SEASON_TYPE'}
            adv_unique_cols = list(adv_cols - base_cols_for_compare - {'SEASON_TYPE'}) # Also exclude SEASON_TYPE here
            cols_to_keep = ['GROUP_ID'] + adv_unique_cols
            cols_to_keep = [col for col in cols_to_keep if col in team_lineup_advanced.columns] # Ensure columns exist

            team_lineup_advanced_subset = team_lineup_advanced[cols_to_keep]

            # Perform the merge
            try:
                team_lineup_merged = pd.merge(
                    team_lineup_base, # Already contains SEASON_TYPE
                    team_lineup_advanced_subset,
                    on='GROUP_ID',
                    how='inner', # Keep only lineups present in both Base and Advanced
                    suffixes=('', '_adv') # Suffix for any unexpected overlaps besides GROUP_ID
                )

                if not team_lineup_merged.empty:
                    team_lineup_merged['team'] = team_name
                    team_lineup_merged['team_id'] = team_id_i
                    merged_data_for_season_type = team_lineup_merged # Store merged data
                    print(f"      Successfully merged {len(team_lineup_merged)} {season_type} lineups.")
                else:
                    print(f"      Warning: No common {season_type} lineups found after merging Base and Advanced.")
                    # Decide if you want to keep base-only data in this case
                    # team_lineup_base['team'] = team_name
                    # team_lineup_base['team_id'] = team_id_i
                    # merged_data_for_season_type = team_lineup_base
                    # print("      Using Base stats only for this season type.")


            except KeyError as e:
                 print(f"      Error merging {season_type} data: Missing key {e}. Skipping merge.")
                 # Optionally add base if merge fails due to key error
                 # team_lineup_base['team'] = team_name
                 # team_lineup_base['team_id'] = team_id_i
                 # merged_data_for_season_type = team_lineup_base
                 # print("      Using Base stats only due to merge error.")


        elif team_lineup_base is not None and not team_lineup_base.empty:
             # Handle case where only base stats were retrieved successfully
             print(f"    Warning: Only Base {season_type} stats retrieved or Advanced stats were empty. Using Base stats only.")
             team_lineup_base['team'] = team_name
             team_lineup_base['team_id'] = team_id_i
             # SEASON_TYPE is already in team_lineup_base
             merged_data_for_season_type = team_lineup_base

        # Add similar elif for advanced if needed, though less common to use alone
        # elif team_lineup_advanced is not None and not team_lineup_advanced.empty:
        #      print(f"    Warning: Only Advanced {season_type} stats retrieved. Using Advanced stats only.")
        #      team_lineup_advanced['team'] = team_name
        #      team_lineup_advanced['team_id'] = team_id_i
        #      merged_data_for_season_type = team_lineup_advanced

        else:
            print(f"    No usable {season_type} data retrieved for team {team_name}.")

        # Concatenate the results for this season_type to the main DataFrame
        if merged_data_for_season_type is not None and not merged_data_for_season_type.empty:
            league_lineup = pd.concat([league_lineup, merged_data_for_season_type], ignore_index=True)
            team_has_data = True # Mark that we got some data for this team

        # Small delay between processing season types for the same team (optional but recommended)
        time.sleep(0.5)

    if not team_has_data:
        print(f"  No data added for team {team_name} for either season type.")

    print(f"Finished processing {team_name}. Pausing before next team...")
    time.sleep(1) # Keep delay between different teams

print("-" * 30)
print("All teams processed.")

# In[5]: Post-processing and Saving
print("\nProcessing final DataFrame...")
if not league_lineup.empty:
    # Create player list from GROUP_NAME (handle potential None/NaN in GROUP_NAME)
    league_lineup['players_list'] = league_lineup['GROUP_NAME'].fillna('').str.split(' - ')

    # Sort by team name and then season type for clarity
    league_lineup = league_lineup.sort_values(by=['team', 'SEASON_TYPE', 'MIN'], ascending=[True, True, False]) # Sort by MIN descending within type

    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)

    # Save to CSV - update filename to reflect content
    output_filename = f'data/NBALineup{target_season.replace("-","")}_RegSeason_Playoffs_BaseAdvanced.csv'
    try:
        league_lineup.to_csv(output_filename, index=False)
        print(f"Data saved successfully to {output_filename}")
        print(f"Final DataFrame shape: {league_lineup.shape}")
        print("\nColumns in the final DataFrame:")
        print(league_lineup.columns.tolist()) # Should include 'SEASON_TYPE'
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")

else:
    print("No data was collected. The final DataFrame is empty.")

print("\nScript finished.")