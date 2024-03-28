import modal


from .common import stub


def get_locations(client):
    query = "SELECT location_name, sales_volume, monthly_fixed_costs, initial_opening_cost FROM locations;"
    locations = []
    for location in client.execute.remote_gen(query):
        locations.append(location)
    return locations


def save_location(
    client, location_name, sales_volume, monthly_fixed_costs, initial_opening_cost
):
    query = """
        INSERT INTO locations (location_name, sales_volume, monthly_fixed_costs, initial_opening_cost)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_name) DO UPDATE SET
        sales_volume = EXCLUDED.sales_volume,
        monthly_fixed_costs = EXCLUDED.monthly_fixed_costs,
        initial_opening_cost = EXCLUDED.initial_opening_cost
    """
    vars = (location_name, sales_volume, monthly_fixed_costs, initial_opening_cost)
    client.execute.remote_gen(query, vars=vars)


@stub.local_entrypoint()
def test_location():
    ApiClient = modal.Cls.lookup("coffeeshop-neon-client", "ApiClient")
    api_client = ApiClient()
    branches = api_client.get_branches.remote()
    branch_id, branch_name = branches[0]
    host = api_client.get_host.remote(branch_id)
    DbClient = modal.Cls.lookup("coffeeshop-neon-client", "DbClient")
    client = DbClient(host)
    print("found locations:", *get_locations(client), sep="\n\t")
