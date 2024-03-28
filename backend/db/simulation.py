import modal


from .common import stub


def save_simulation(simulation_name, simulation_results, client):
    for ii, net_profit in enumerate(simulation_results, start=1):
        try:
            next(
                client.execute.remote_gen(
                    query="""INSERT INTO cumulative_net_profits (simulation_name, week, cumulative_net_profit)
                             VALUES (%s, %s,%s)""",
                    vars=(simulation_name, ii, float(net_profit)),
                )
            )
        except StopIteration:
            continue


def fetch_simulation_names(client):
    rows = [
        row
        for row in client.execute.remote_gen(
            "SELECT DISTINCT simulation_name FROM cumulative_net_profits ORDER BY simulation_name",
        )
    ]
    return [row[0] for row in rows]


def fetch_simulation(simulation_name, host, client):
    rows = []
    query = """
        SELECT week, cumulative_net_profit
        FROM cumulative_net_profits
        WHERE simulation_name = %s
        ORDER BY week
    """
    vars = (simulation_name,)
    for row in client.execute.remote_gen(query, vars=vars):
        rows.append(row)
    return rows


@stub.local_entrypoint()
def test_simulation():
    ApiClient = modal.Cls.lookup("coffeeshop-neon-client", "ApiClient")
    api_client = ApiClient()
    branches = api_client.get_branches.remote()
    branch_id, branch_name = branches[0]
    host = api_client.get_host.remote(branch_id)

    DbClient = modal.Cls.lookup("coffeeshop-neon-client", "DbClient")
    client = DbClient(host)
    simulation_names = fetch_simulation_names(client)
    if simulation_names:
        print(
            f"{len(simulation_names)} simulations found:",
            *simulation_names,
            sep="\n\t",
        )
    else:
        print("no simulations found")
