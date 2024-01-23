# imports
from flask import Flask, render_template, request, flash
import phenotype_utils

app = Flask(__name__)
app.secret_key = "test_app_gpt"

@app.route("/diseases")
def index():
    return render_template("index.html")


@app.route("/query", methods=["GET"])
def submit_phenotypes():
    # initialize 
    map_result = {}
    phenotypes = None
    
    # get the input
    if request.args.get('phenotypes'):
        phenotypes = str(request.args.get('phenotypes'))

    print("got request: {} with inputs: {}".format(request.method, phenotypes))


    # split the genes into list
    if phenotypes:
        # build the phenotype list
        list_temp = phenotypes.split(",")
        list_select = []

        for value in list_temp:
            gene = value.strip()
            print("got phenotype: -{}-".format(gene))
            list_select.append(gene)

        # get the db connection
        conn = phenotype_utils.get_connection()

        # get the result diseases
        # map_disease = phenotype_utils.get_disease_score_for_phenotype_list(conn=conn, list_curies=list_select, log=False)
        list_disease = phenotype_utils.get_disease_score_sorted_list_for_phenotype_list(conn=conn, list_curies=list_select, log=False)
        print("got disease list size of: {}".format(len(list_disease)))

        # add to map
        # map_result['results'] = map_disease
        map_result['results'] = list_disease

    # return
    return map_result


    #     list_temp = input_genes.split(",")
    #     list_select = []

    #     for value in list_temp:
    #         gene = value.strip()
    #         print("got gene: -{}-".format(gene))
    #         list_select.append(gene)

    #     # get the data
    #     conn = get_connection()
    #     # list_abstracts = get_list_abstracts(conn=conn, list_genes=list_select, log=True)
    #     map_gene_abstracts = get_map_gene_abstracts(conn=conn, list_genes=list_select, log=True)

    #     if map_gene_abstracts and len(map_gene_abstracts) > 0:
    #         # build the prompt inputs
    #         str_gene = ",".join(map_gene_abstracts.keys())
    #         # str_abstract = "\n".join(list_abstract_llm)

    #         print("found summaries for genes: {}".format(str_gene))
    #         # print("\ngot abstracts: {}".format(str_abstract))

    #         # call the LLM
    #         # biology_abstract = ml_utils.call_llm(prompt_template=ml_utils.PROMPT_BIOLOGY, str_gene=str_gene, str_abstract=str_abstract, log=True)
    #         biology_abstract = ml_utils.call_gene_abstract_llm_recurisve(prompt_template=ml_utils.PROMPT_BIOLOGY, map_gene_abstracts=map_gene_abstracts, max_tokens=2500, log=True)
    #         print("\n\ngot biology LLM result: \n{}".format(biology_abstract))

    #         list_gene_llm = list(map_gene_abstracts.keys())
    #         # # call the LLM
    #         # pathway_abstract = ml_utils.call_llm(prompt_template=ml_utils.PROMPT_PATHWAYS, str_gene=str_gene, str_abstract=str_abstract, log=True)
    #         # print("\n\ngot biology LLM result: \n{}".format(biology_abstract))

    # else:
    #     print("no input genes")

    # # add data for return 
    # flash(list_select, 'list_genes_input')
    # flash(list_gene_llm, 'list_genes_used')
    # flash(biology_abstract, 'abstract_biology')
    # flash(pathway_abstract, 'abstract_pathway')
    # # flash(list_genes_missing, 'list_missing')
    # # flash(prompt_gpt, 'prompt')
    # # flash(abstract_gpt, 'abstract')

    # return render_template("index.html")


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8082)
