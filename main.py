import networkx as nx
import graph_tool.all as gt
import json
import numpy as np
from sklearn import decomposition
from datetime import datetime
import igraph as ig
# May 1 2015 - 396,770 - 1430470800
# May 2 2015 - 2,162,379 - 1430557200
# May 3 2015 - 3,675,445 - 1430643600
# May 4 2015 - 5,291,141 - 1430730000
# May 5 2015 - 7,159,258 - 1430816400
# May 6 2015 - 9,010,670 - 1430902800
# May 7 2015 - 1,096,2091 - 1430989200
# May 8 2015 - 12,857,857 - 1431075600
# May 9 2015 - 14,662,856 - 1431162000
# May 10 2015 - 16,187,162 - 1431248400
# May 11 2015 - 17,786,014 - 1431334800
# May 12 2015 - 19,551,302 - 1431421200
# May 13 2015 - 21,501,421 - 1431507600
# May 14 2015 - 23,407,590 - 1431594000
# May 15 2015 - 25,247,740 - 1431680400
# May 16 2015 - 27,009,589 - 1431766800
# May 17 2015 - 28,515,732 - 1431853200
# May 18 2015 - 30,134,749 - 1431939600
# May 19 2015 - 31,964,194 - 1432026000
# May 20 2015 - 33,868,378 - 1432112400
# May 21 2015 - 35,784,395 - 1432198800
# May 22 2015 - 37,590,488 - 1432285200
# May 23 2015 - 39,304,331 - 1432371600
# May 24 2015 - 40,778,728 - 1432458000
# May 25 2015 - 42,322,504 - 1432544400
# May 26 2015 - 44,032,508 - 1432630800
# May 27 2015 - 45,946,268 - 1432717200
# May 28 2015 - 47,860,600 - 1432803600
# May 29 2015 - 49,778,729 - 1432890000
# May 30 2015 - 51,670,014 - 1432976400
# May 31 2015 - 53,205,912 - 1433062800

reload = True

def remove_uneeded_comments():
    idMappings = {}
    with open("RC_2015-05") as fileobject:
        x=0
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)

            json_loaded = json.loads(doc)
            x = x + 1
            if(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            else:
                idMappings[json_loaded["name"]] = json_loaded["author"]
        print("Nodes added")
    with open("RC_2015-05") as fileobject:
        x = 0
        f = open("cleaned_reddit.txt", 'w')
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            x = x + 1
            json_loaded = json.loads(doc)
            if(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            elif (json_loaded["parent_id"] in idMappings):
                f.write(doc)

def reformat_comment_data():
    idMappings = {}
    with open("cleaned_reddit.txt", 'r') as fileobject:
        x=0
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            json_loaded = json.loads(doc)
            x = x + 1
            idMappings[json_loaded["name"]] = json_loaded["author"]
    with open("cleaned_reddit.txt", 'r') as fileobject:
        f = open("parent_added.txt", 'w')
        x=0
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            json_loaded = json.loads(doc)
            x = x + 1
            if json_loaded["parent_id"] in idMappings:
                json_loaded["parent_author"] = idMappings[json_loaded["parent_id"]]
                json.dump(json_loaded, f)
                f.write("\n")
            else:
                continue


def new_network_gen(start_time, end_time):
    x=0
    nxGraph = nx.DiGraph()
    with open("parent_added.txt") as fileobject:
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            x+=1
            json_loaded = json.loads(doc)
            if(json_loaded["created_utc"] < start_time):
                continue
            elif(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            elif(json_loaded["parent_author"] == "automoderator" or json_loaded["parent_author"] == "[deleted]"):
                continue
            elif (json_loaded["created_utc"] >= end_time):
                break
            else:
                nxGraph.add_node(json_loaded["author"])
                # nxGraph.add_node(json_loaded["parent_author"])
                if not json_loaded["parent_author"] in nxGraph:
                    continue
                if nxGraph.has_edge(json_loaded["author"], json_loaded["parent_author"]):
                    nxGraph[json_loaded["author"]][json_loaded["parent_author"]]["weight"] += 1
                else:
                    nxGraph.add_edge(json_loaded["author"], json_loaded["parent_author"])
                    nxGraph[json_loaded["author"]][json_loaded["parent_author"]]["weight"] = 1
    print("removing")
    for n in nxGraph.nodes():
        if (nxGraph.degree(n) == 0):
            nxGraph.remove_node(n)
    print("writing")
    nx.write_graphml(nxGraph, "graph.graphml")
    return nxGraph

def generate_networkx_network(start_time, end_time):
    x = 0
    nxGraph = nx.DiGraph()
    idMappings = {}
    users = set()
    with open("parent_added.txt") as fileobject:
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            json_loaded = json.loads(doc)
            x = x + 1
            if(json_loaded["created_utc"] < start_time):
                continue
            elif(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            elif (json_loaded["created_utc"] >= end_time):
                break
            else:
                nxGraph.add_node(json_loaded["author"])
                users.add(json_loaded["author"])
                idMappings[json_loaded["name"]] = json_loaded["author"]
        print("Nodes added")
    print(len(users))
    with open("parent_added.txt") as fileobject:
        x = 0
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            x = x + 1
            json_loaded = json.loads(doc)
            if(json_loaded["created_utc"] < start_time):
                print("early")
                continue
            elif(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            elif (json_loaded["created_utc"] >= end_time):
                break
            if (json_loaded["parent_id"] in idMappings):
                if nxGraph.has_edge(json_loaded["author"], idMappings[json_loaded["parent_id"]]):
                    nxGraph[json_loaded["author"]][idMappings[json_loaded["parent_id"]]]["weight"] += 1
                else:
                    nxGraph.add_edge(json_loaded["author"], idMappings[json_loaded["parent_id"]])
                    nxGraph[json_loaded["author"]][idMappings[json_loaded["parent_id"]]]["weight"] = 1
                    #         edges += [(str(doc["author"]),str(idMappings[doc["parent_id"]]))]
    print("removing")
    for n in nxGraph.nodes():
        if (nxGraph.degree(n) == 0):
            nxGraph.remove_node(n)
    print("writing")
    nx.write_graphml(nxGraph, "graph.graphml")
    return nxGraph

def print_date_locations():
    x = 0
    cur = 0
    with open("parent_added.txt") as fileobject:
        for line in fileobject:
            parsed_date = datetime.fromtimestamp(float(json.loads(line)['created_utc']))
            if (parsed_date.day != cur):
                print
                print(x)
                print(parsed_date)
                print
                cur = parsed_date.day
            if (x % 500000 == 0):
                print(x)
            x = x + 1

def load_network_networkx(start_time, end_time):
    print("Trying to load GraphML file")
    try:
        graph = nx.read_graphml("graph.graphml")
        print(graph.order())
        return graph
    except IOError:
        print("File not found. Recreating network")
        return generate_networkx_network(start_time, end_time)

def graph_tool_statistics():
    print("loading")
    graph = gt.load_graph("graph.graphml")
    print("computing")
    print(gt.global_clustering(graph))

def print_statistics(graph):
    print("computing statistics")
    numNodes = graph.order()
    numEdges = graph.size(weight="weight")
    averageDegree = float(numEdges) / numNodes
    print("Number of Nodes: ", numNodes)
    print("Number of Edges: ", numEdges)
    print("Average Degree: ", averageDegree)
    graph_tool_statistics()

def row_normalize(arr):
    row_sums = arr.sum(axis=1)
    new_matrix = arr / row_sums[:, np.newaxis]
    return new_matrix

def col_normalize(arr):
    col_averages = np.mean(arr,0)
    new_matrix = arr - col_averages
    return new_matrix

def find_triangles(graph):
    triangles = []
    x = 0
    for e in graph.edges_iter():
        if x % 1000 == 0:
            print(x)
        x+=1
        for n in graph.nodes_iter():
            if graph.has_edge(n,e[0]) and graph.has_edge(e[1],n):
                triangles.append((e[0],e[1],n))
    print(len(triangles))

def load_to_igraph():
    graph = ig.load("graph.graphml")
    # print(graph)
    ig.Graph.community_multilevel(graph)
    return graph

def write_adjacency_list(graph):
    print(5)

def create_feature_vector(start_time, end_time):
    subreddit_indicies = {}
    user_indicies = {}
    s_x = 0;
    u_x = 0;
    x = 0
    with open("parent_added.txt") as fileobject:
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            x+=1
            json_loaded = json.loads(doc)
            if(json_loaded["created_utc"] < start_time):
                continue
            elif(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            elif (json_loaded["created_utc"] >= end_time):
                break
            if not json_loaded["author"] in user_indicies:
                user_indicies[json_loaded["author"]] = u_x
                u_x+=1
            if not json_loaded["subreddit"] in subreddit_indicies:
                subreddit_indicies[json_loaded["subreddit"]] = s_x
                s_x+=1
    arr = np.zeros((len(user_indicies), len(subreddit_indicies)))
    x=0
    with open("parent_added.txt") as fileobject:
        for doc in fileobject:
            if x % 50000 == 0:
                print(x)
            x+=1
            json_loaded = json.loads(doc)
            if(json_loaded["created_utc"] < start_time):
                continue
            elif(json_loaded["author"] == "automoderator" or json_loaded["author"] == "[deleted]"):
                continue
            elif (json_loaded["created_utc"] >= end_time):
                break
            arr[user_indicies[json_loaded["author"]]][subreddit_indicies[json_loaded["subreddit"]]]+=1
    subreddit_indicies = {}
    user_indicies = {}
    normalized = col_normalize(arr)
    print(normalized)
    # print(np.mean(normalized,0))
    # pca = decomposition.PCA(n_components=7)
    # reduced = pca.fit_transform(normalized)
    # pca = decomposition.PCA()
    # pca.fit(normalized)
    # print(pca.explained_variance_ratio_)
    # print(pca)
    # print()
    # print("variance")
    # print(pca.explained_variance_ratio_)
    # print()
    # print("reduced")
    # print(reduced)
    # print()
    # print(pca.explained_variance_ratio_.cumsum())

def calculate_modularity(graph, communities, community_id, nodeToAdd):
    degreeInCommunity = 0.0
    totalEdgesIntoCommunity = 0.0
    totalExternalEdges = 0.0
    total_edges = graph.size()
    node_in_degree = float(graph.in_degree(nodeToAdd))
    node_out_degree = float(graph.out_degree(nodeToAdd))
    if(graph.has_edge("a", "a")):
        degreeInCommunity +=2
    for node in communities[community_id]:
        if(node == nodeToAdd):
            continue
        if (graph.has_edge(node, nodeToAdd)):
            degreeInCommunity += 1
        if(graph.has_edge(nodeToAdd, node)):
            degreeInCommunity += 1
        for incoming in graph.in_edges(node):
            if not incoming[0] in communities[community_id]:
                totalEdgesIntoCommunity += 1
        for outgoing in graph.out_edges(node):
            if not outgoing[1] in communities[community_id]:
                totalExternalEdges += 1
    return ((degreeInCommunity / float(total_edges)) - ((node_out_degree * totalEdgesIntoCommunity) + (node_in_degree * totalExternalEdges))/(float(total_edges) * float(total_edges)))


def louvain_modularity(graph):
    communityMapping = {}
    communities = {}
    community_id = 0
    for n in graph.nodes_iter():
        communityMapping[n] = community_id
        communities[community_id] = set()
        communities[community_id].add(n)
        community_id += 1
    changeMade = True
    iteration_count = 0
    while changeMade:
        print("Iteration: ",iteration_count)
        changeMade = False
        x = 0
        for n in graph. nodes_iter():
            if(x % 10 == 0):
                print(x)
            x += 1
            best_modularity = calculate_modularity(graph, communities, communityMapping[n], n)
            best_index = communityMapping[n]
            for incoming_edge in graph.in_edges(n):
                id_for_node = communityMapping[incoming_edge[0]]
                if id_for_node == communityMapping[n]:
                    continue
                modularity = calculate_modularity(graph, communities, id_for_node, n)
                if modularity > best_modularity:
                    best_index = id_for_node
                    best_modularity = modularity

            for outgoing_edge in graph.in_edges(n):
                id_for_node = communityMapping[outgoing_edge[1]]
                if id_for_node == communityMapping[n]:
                    continue
                modularity = calculate_modularity(graph, communities, id_for_node, n)
                if modularity > best_modularity:
                    best_index = id_for_node
                    best_modularity = modularity
            if not best_index == communityMapping[n]:
                changeMade = True
                old_index = communityMapping[n]
                communityMapping[n] = best_index
                communities[old_index].remove(n)
                communities[best_index].add(n)
        iteration_count += 1
    return communities

def test_network():
    graph = nx.DiGraph()
    graph.add_node(1)
    graph.add_node(2)
    graph.add_node(3)
    graph.add_node(4)
    graph.add_node(5)
    graph.add_node(6)
    graph.add_edge(1,2)
    graph.add_edge(2,1)
    graph.add_edge(1,3)
    graph.add_edge(3,2)
    graph.add_edge(3,4)
    graph.add_edge(4,5)
    graph.add_edge(4,6)
    graph.add_edge(5,4)
    graph.add_edge(5,6)
    graph.add_edge(6,5)
    return graph

def main():
    graph = None
    start_time = "1430470800"
    end_time = "1430557200"
    # if(not reload):
    #     graph = load_network_networkx(start_time,end_time)
    # else:
    #     graph = generate_networkx_network(start_time,end_time)
    # print_statistics(graph)
    graph = new_network_gen(start_time, end_time)
    # load_to_igraph()
    # find_triangles(graph)
    # graph = new_network_gen(start_time, end_time)
    # louvain_modularity(graph)
    print(len(louvain_modularity(graph)))
    # print_statistics(graph)
    # reformat_comment_data()
    # test()
    # graph_tool_statistics()
    # create_feature_vector(start_time, end_time)
if __name__ == "__main__":
    main()
