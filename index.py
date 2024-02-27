import os
import csv
import requests
import difflib  
import json
from functools import reduce
from dotenv import load_dotenv
load_dotenv()


class ZenDub:

    records = {
        "pipelines": None,
        "users": None,
        "workspaces": None,
        "workspace": None,
        "tickets": None,
        "repo": None
    }

    config = {
        "graphql_endpoint": 'https://api.zenhub.com/public/graphql',
        "zenhub_token": os.getenv('ZENHUB_TOKEN')
    }    

    def __init__(self):
        ## zenhub API auth - use env var or ask for token
        if 'zenhub_token' not in self.config or self.config['zenhub_token'] is None:
            self.config['zenhub_token'] = self.gather_user_input('Zenhub API Token?')
        self.set_zenhub_headers()

        ## workspace - use recent or search. use single workspace or ask
        self.records['workspaces'] = self.query_recent_workspaces()
        if not self.records['workspaces']:
            self.records['workspaces'] = self.search_workspaces(
                self.gather_user_input('Workspace name search?', 'mobile')
            )
        if len(self.records['workspaces']) > 1:
            self.records['workspace'] = self.gather_user_choice(
                "Select a workspace",
                self.records['workspaces'],
                1,
                'name'
            )
        else: 
            self.records['workspace'] = self.records['workspaces'][0]
            print(f"Using workspace: {self.records['workspace']['name']}\n")
        self.config['workspace_id'] = self.records['workspace']['id']
        
        ## get pipelines
        self.records['pipelines'] = self.records['workspace']['pipelinesConnection']['nodes']
        pipelinesNames = [f"{record['name']}" for record in self.records['pipelines']]
        print(f"Got pipelines:{pipelinesNames}\n")

        ## get assignees
        self.records['assignees'] = self.records['workspace']['assignees']['nodes']
        assigneesNames = [self.get_path(
            record,
            'zenhubUser.githubUser.login'
        ) for record in self.records['assignees'] if self.get_path(record, 'zenhubUser.githubUser.login')]
        print(f"Got assignees:{assigneesNames}\n")

        ## read user map
        user_map_file = 'users.csv'
        if not os.path.exists(user_map_file):
            user_map_file = self.gather_user_input('User map file', user_map_file)
        print(f"Using user map {user_map_file}\n")
        self.records['users'] = self.load_username_map(user_map_file)

        ## use single repo or ask user to select repo
        if len(self.records['workspace']['repositoriesConnection']['nodes']) > 1:
            self.records['repo'] = self.gather_user_choice(
                "Select a repo",
                self.records['workspace']['repositoriesConnection']['nodes'],
                1,
                'name'
            )
        else:
            self.records['repo'] = self.records['workspace']['repositoriesConnection']['nodes'][0]
            print(f"Using repo: {self.records['repo']['name']}\n")
        self.config['repository_id'] = self.records['repo']['id']
        self.config['repository_ghid'] = self.records['repo']['ghId']

        ## read tickets, insert new issues
        tickets_file = 'tickets.csv'
        if not os.path.exists(tickets_file):
            tickets_file = self.gather_user_input('Tickets CSV file', tickets_file)
        print(f"Using tickets file {tickets_file}\n")
        self.records['tickets'] = self.process_assembla_csv(tickets_file)
        for ticket in self.records['tickets']:

            ## insert issue
            res = self.create_zenhub_issue(
                self.config['repository_id'],
                ticket
            )

            ## set pipeline
            ticket['id'] = res['createIssue']['issue']['id']
            ticket['number'] = res['createIssue']['issue']['number']
            self.set_pipeline(
                self.config['workspace_id'],
                self.match_pipeline(ticket['status']),
                ticket['id']
            )

            ## check work
            check_id = self.get_issue(
                self.config['repository_id'],
                ticket['number']
            )
            if (check_id == ticket['id']):
                print(f"Ticket {ticket['number']} created!")
            else:
                raise Exception(f"Ticket no created {ticket['number']}!")
                


## util methods

    def get_path(self, data, path, default=None):
        try:
            return reduce(lambda d, key: d[key], path.split('.'), data)
        except (KeyError, TypeError):
            return default  # or any default value you prefer

    def set_zenhub_headers(self):
        self.config["zenhub_headers"] = {
            'Authorization': f"Bearer {self.config['zenhub_token']}",
            'Content-Type': 'application/json'
        }

    def match_pipeline(self, status, pipelines=None):
        pipelines = pipelines or self.records['pipelines']
        names = [item['name'] for item in pipelines]
        matches = difflib.get_close_matches(status, names, n=1, cutoff=0.0)
        if matches:
            match_name = matches[0]
            for item in pipelines:
                if item['name'] == match_name:
                    return item
        return self.gather_user_choice(f"select pipeline for ticket status: {status}", pipelines)

    def args_to_json_array(self, *args):
        result = []
        for arg in args:
            if isinstance(arg, list):
                result.extend(arg)
            elif isinstance(arg, str) and ',' in arg:
                # If the argument is a comma-delimited string, convert to list and extend
                result.extend(arg.split(','))
            else:
                result.append(f"{arg}")
        return json.dumps(result)

    def load_username_map(self, csv_path):
        user_mapping = {}
        with open(csv_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                user_mapping[row['assembla']] = row['github']
        print(f"{user_mapping}\n")
        return user_mapping

    def lookup_github_login(self, assembla_user):
        return self.records['users'].get(assembla_user, None)

    def process_assembla_csv(self, assembla_csv_path):
        tickets = []
        with open(assembla_csv_path, mode='r', encoding='utf-8') as csvfile: 
            reader = csv.DictReader(csvfile)
            for row in reader:
                tickets.append(row)
        return tickets

    def gather_user_input(self, prompt_message, default=None):
        if default is not None:
            full_prompt = f"{prompt_message} (default: {default}): "
        else:
            full_prompt = prompt_message
        user_input = input(full_prompt)
        return user_input if user_input != "" else default

    def gather_user_choice(self, prompt_message, choices, default=None, choice_preview=None):
        print(prompt_message)    
        for i, choice in enumerate(choices, start=1):
            if choice_preview:
                choice = choice[choice_preview]
            print(f"{i}. {choice}")
        while True:
            user_input = input(f"({default}):")
            if user_input == "" and default is not None:
                return choices[default - 1]
            try:
                user_selection = int(user_input)
                if 1 <= user_selection <= len(choices):
                    return choices[user_selection - 1]
                else:
                    print(f"Enter a number between 1 and {len(choices)}.")
            except ValueError:
                print("Enter a valid number.")

    def execute_graphql_query(self, query, variables={}, record_name=None):
        response = requests.post(
            self.config['graphql_endpoint'],
            headers=self.config['zenhub_headers'],
            json={'query': query,
            'variables': variables
        })
        if response.status_code == 200:
            res = response.json()
            if 'data' in res:
                res = res['data']
            if record_name:
                self.records[record_name] = res
            else:
                return res
        else:
            print(f"GraphQL Error: {response.text}\n")


## queries and mutations

    def create_zenhub_issue(self, repository_id, ticket):
        labels = self.args_to_json_array(ticket['tag_names'], 'PD-'+ticket['number'])
        assignees = self.args_to_json_array(self.lookup_github_login(ticket.get('assigned_to_name', None)))

        mutation = """
            mutation createZenhubIssue(
                $repositoryId: ID!,
                $title: String!,
                $body: String
            ) {
            createIssue(input: {
                    repositoryId: $repositoryId,
                    title: $title,
                    body: $body,
                    assignees: %s,
                    labels: %s,
                }) {
                    issue {
                        id
                        number
                    }
                }
            }
        """ % (assignees, labels)

        variables = {
            'repositoryId': repository_id,
            'title': ticket['summary'],
            'body': ticket['description'],
            'estimate': ticket['estimate']
        }

        res = self.execute_graphql_query(mutation, variables)        
        if res:
            print(f"ZenHub createIssue Response: {res} - {ticket['summary']}\n")
        else:
            print(f"Failed to create ZenHub issue. {res}\n")
        return res

    def set_pipeline(self, workspace_id, pipeline, issue_id):
        mutation = """
            mutation moveIssue($moveIssueInput: MoveIssueInput!, $workspaceId: ID!) {
                moveIssue(input: $moveIssueInput) {
                    issue {
                        id
                        pipelineIssue(workspaceId: $workspaceId) {
                            priority {
                                id
                                name
                                color
                            }
                            pipeline {
                                id
                            }
                        }
                    }
                }
            }
        """
        variables = {
            "workspaceId": workspace_id,
            "moveIssueInput": {
                "pipelineId": pipeline['id'],
                "issueId": issue_id,
                "position": 0
            }
        }
        res = self.execute_graphql_query(mutation, variables)
        print(f"{res}\n")
        return res  

    def search_workspaces(self, workspace_name):
        query = """
        query {
            viewer {
                id
                searchWorkspaces(query: "%s") {
                    nodes {
                        id
                        name
                        assignees {
                            nodes {
                                id
                                ghId
                                name
                                zenhubUser {
                                    id
                                    contactEmail
                                    email
                                    githubUser {
                                        id
                                        login
                                        name
                                        ghId
                                    }
                                }
                            }
                        }
                        pipelinesConnection {
                            nodes {
                                id
                                name
                            }
                        }
                        repositoriesConnection {
                            nodes {
                                id
                                name
                                ghId
                            }
                        }
                    }
                }
            }
        }
        """ % workspace_name
        res = self.execute_graphql_query(query)
        return res['viewer']['searchWorkspaces']['nodes']

    def query_recent_workspaces(self):
        query = """
            query {
                recentlyViewedWorkspaces {
                    nodes {
                            id
                            name
                            assignees {
                                nodes {
                                    id
                                    ghId
                                    name
                                    zenhubUser {
                                        id
                                        contactEmail
                                        email
                                        githubUser {
                                            id
                                            login
                                            name
                                            ghId
                                        }
                                    }
                                }
                            }
                            pipelinesConnection {
                                nodes {
                                    id
                                    name
                                }
                            }
                            repositoriesConnection {
                                nodes {
                                    id
                                    name
                                    ghId
                                }
                            }
                        }
                }
            }
        """
        res = self.execute_graphql_query(query)
        res = res['recentlyViewedWorkspaces']['nodes']
        return res

    def get_issue(self, repository_id, issue_number):
        query = """
        query GetIssueByInfo($repositoryId: ID!, $issueNumber: Int!) {
            issueByInfo(repositoryId: $repositoryId, issueNumber: $issueNumber) {
                id
                number
                title
                pipelineIssues {
                    edges {
                        node {
                            pipeline {
                                id
                                name
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {
            'repositoryId': repository_id,
            'issueNumber': issue_number
        }
        res = self.execute_graphql_query(query, variables)
        return res['issueByInfo']['id']

# go!
ZenDub()

