import os
import re
import codecs
from lxml import etree
from copy import deepcopy
import time
import httpx
import regex as re
from langchain_aws import ChatBedrock
from langchain_openai import AzureChatOpenAI
import os
import time
import httpx
import regex as re
from langchain_aws import ChatBedrock
from langchain_openai import AzureChatOpenAI
from botocore.config import Config



def sanitize_xml_file(src_path):
    with open(src_path, "rb") as f:
        data = f.read()
    if data.startswith(codecs.BOM_UTF8):
        data = data[len(codecs.BOM_UTF8):]
    text = data.decode("utf-8", errors="ignore").lstrip()
    
    decl_pattern = re.compile(r"<\?xml[^?]*\?>", flags=re.IGNORECASE)
    seen_first = [False]

    def keep_first_decl(m):
        if not seen_first[0]:
            seen_first[0] = True
            return m.group(0) if m.start() == 0 else ""
        return ""

    return decl_pattern.sub(keep_first_decl, text)

def chunk_xml_to_dict(input_xml_path, line_threshold=100):
    """
    Recursively splits XML and returns a dictionary: { "chunk_1": "<?xml...", ... }
    """
    xml_content = sanitize_xml_file(input_xml_path)
    
    try:
        parser = etree.XMLParser(huge_tree=True)
        root = etree.fromstring(xml_content.encode("utf-8"), parser=parser)
    except etree.XMLSyntaxError:
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(xml_content.encode("utf-8"), parser=parser)

    chunks_dict = {}
    chunk_counter = [1]

    def element_line_count(element):
        raw_xml = etree.tostring(element, pretty_print=True).decode("utf-8")
        return len(raw_xml.splitlines())

    def get_ancestor_tree(element):
        ancestors = []
        curr = element.getparent()
        while curr is not None:
            ancestors.insert(0, curr)
            curr = curr.getparent()
        if not ancestors:
            return etree.Element(element.tag, **element.attrib), None
        new_root = etree.Element(ancestors[0].tag, **ancestors[0].attrib)
        current_parent = new_root
        for anc in ancestors[1:]:
            current_parent = etree.SubElement(current_parent, anc.tag, **anc.attrib)
        return new_root, current_parent

    def add_to_dict(element_tree_root):
        chunk_key = f"chunk_{chunk_counter[0]}"
        xml_string = etree.tostring(
            element_tree_root,
            pretty_print=True,
            xml_declaration=True,
            encoding="UTF-8"
        ).decode("utf-8")
        chunks_dict[chunk_key] = xml_string
        chunk_counter[0] += 1

    def process_element(element):
        if element_line_count(element) <= line_threshold or len(element) == 0:
            new_root, leaf_parent = get_ancestor_tree(element)
            if leaf_parent is not None:
                leaf_parent.append(deepcopy(element))
                add_to_dict(new_root)
            else:
                add_to_dict(deepcopy(element))
            return

        with_gc = [c for c in element if len(c) > 0]
        without_gc = [c for c in element if len(c) == 0]

        if without_gc:
            new_root, leaf_parent = get_ancestor_tree(element)
            parent_copy = etree.Element(element.tag, **element.attrib)
            for ch in without_gc:
                parent_copy.append(deepcopy(ch))
            if leaf_parent is not None:
                leaf_parent.append(parent_copy)
                add_to_dict(new_root)
            else:
                add_to_dict(parent_copy)

        for child in with_gc:
            process_element(child)

    for top_level_item in root:
        process_element(top_level_item)

    return chunks_dict

# Usage:
# result = chunk_xml_to_dict("/home/admin/kamal_code_repo/XML_intermeditate.xml", line_threshold=100)
# print(result.values())

# Initialize LLMs (Keep your existing configurations exactly)

def generate_pseudo_code_one_by_one_chunk(chunks_dict):
    """
    Modified to take a dictionary as input and return a dictionary of results.
    """
    final_results = {}

    llm_v4 = ChatBedrock(
    model_id="...............",
    region_name=os.getenv("AWS_REGION", "u..."),
    streaming=True,
    model_kwargs={"temperature": 0.0, "max_tokens": 65536}
    )

    custom_timeout = httpx.Timeout(
        connect=30000.0, read=6000.0, write=6000.0, pool=6000.0
    )

    AZURE_INFERENCE_CREDENTIAL = "............................"
    AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = "g........1"

    llm_v35 = AzureChatOpenAI(
        azure_endpoint="..........................m",
        azure_deployment=AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,
        api_key=AZURE_INFERENCE_CREDENTIAL,
        api_version="2024-02-01",
        streaming=True,
        temperature=0.0,
        http_client=httpx.Client(timeout=custom_timeout)
    )

    
    print(f"total chunks created : {len(list(chunks_dict.keys()))}")
    # Iterate through dictionary items using index for the LLM switching logic
    for i, (chunk_id, xml_string) in enumerate(chunks_dict.items()):
        start_time = time.time()  
        
        prompt = f"""
        ROLE: Expert informatica XML Architect & Pseudo-code Generator of the same as per below rules.
        
        --- MANDATORY REFINEMENT RULES ---
        1. NO OMISSIONS: You must output the ENTIRE integrated pseudo-code. Do not use placeholders like "// ... (unchanged)" or "Logic remains the same." Every stage from the existing code must be present in the final output along with the new logic.
        2. LOGICAL MAPPING: Identify the object type (Transformer, Lookup, Connector, Hashed File) and insert it into the correct sequential flow (Source -> Processing -> Target).
        3. ATTR-VALUE MAPPING: Map XML 'Property' name/value pairs directly to pseudo-code variables. 
        - Example: <Property Name="LinkName">In_Data</Property> becomes "Input Link: In_Data".
        4. CLEANUP: Strip all UI-specific metadata (GUI coordinates, Canvas IDs, font settings, or color codes). Focus strictly on data transformation and movement.
        5. STABILITY: Ensure all previously defined stages and links are maintained to prevent "forgetting" logic from earlier iterations.
        6. Capture the descriptive overall flow (ex. connection building, datafetching, transformation, processing, cleaning, storage).
        7. Keep in mind to capture all the important steps (transformation, ETL steps, validation steps, business logics, error handling steps etc) as thsi pseudo code will be used to convert it into other programming language like python, pyspark, snowpark etc.
        8. Focus mainly on logical part (which is important part of dataflow) not details of text or description of variables etc
        9. Add link in the pseudo code which makes flow clear previous and next link.
        10. Do not add any informal text of description apart from pseudo code.
        11. While capturing the details of columns definition. Only capture the technical metadata which is important for code conversion in python/pyspark/snowpark and one line.
        12. If contains a SQL Query (Select, Insert, Update, or Join logic), do not decompose or analyze its internal logic. Pass it into the pseudo code as it is such that while converting pseudo code it will be executed as it is and output will be consumed in next links.
        13. If found language other than english, do not consider it in pseudo code generation.
        14. Parent tag is defined for every chunk take note of the parent tags, also add those parent/child tag and input/output links in final pseudo code and preserve the parent/child heirarchy.
        15. Take all the columns in pseudo code which are described.
        
        TASK: Generate the PSEUDO CODE of given XML code chunk so that it captures all the details of XML chunk.
        Below is the chunk :\n{xml_string}
        """

        # Keep your existing model switching logic
        if (i // 5) % 2 == 0:
            current_llm = llm_v4
        else:
            current_llm = llm_v4

        # Invoke LLM and store in dict
        response = current_llm.invoke(prompt).content.strip()
        final_results[chunk_id] = response
        
        total_time = time.time() - start_time
        print(f"--- Processed {chunk_id} --- Time: {total_time:.2f}s")
        
    return final_results

# Example execution:
# input_dict = { "chunk_1": "...", "chunk_2": "..." }
# output_dict = generate_workflow_logic_from_dict(input_dict)


def generate_batch_pseudo_code(chunks_dict, batch_size=10):
    # Instead of reading from a folder, we use the dictionary keys
    chunk_keys = list(chunks_dict.keys())
    total_chunks = len(chunk_keys)
    final_merged_output = {}

    llm_v4 = ChatBedrock(
    model_id="....................0",
    region_name=os.getenv("AWS_REGION", "..2"),
    streaming=True,
    model_kwargs={"temperature": 0.0, "max_tokens": 65536}
    )

    custom_timeout = httpx.Timeout(
        connect=30000.0, read=6000.0, write=6000.0, pool=6000.0
    )

    AZURE_INFERENCE_CREDENTIAL = "............."
    AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = ".....-4.1"

    llm_v35 = AzureChatOpenAI(
        azure_endpoint=".............",
        azure_deployment=AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,
        api_key=AZURE_INFERENCE_CREDENTIAL,
        api_version="..1",
        streaming=True,
        temperature=0.0,
        http_client=httpx.Client(timeout=custom_timeout)
    )
    
    for index, batch_start in enumerate(range(0, total_chunks, batch_size)):
        print(f"Processing batch {index}, starting at index {batch_start}")
        batch_end = min(batch_start + batch_size, total_chunks)
        print(f"Processing chunks from {batch_start} to {batch_end}")
        output_key = f"chunk_{batch_start}_to_{batch_end-1}"
        
        # Concatenate pseudo-code from the input dictionary
        combined_chunks = ""
        for i in range(batch_start, batch_end):
            chunk_id = chunk_keys[i]
            xml_content = chunks_dict[chunk_id]
            combined_chunks += f"\n--- Chunk {i+1} ({chunk_id}) ---\n{xml_content}\n"
             
        prompt = f"""Merge this pseudo code of 5 chunks together of datastage XML code and generate the overall merged rearranged pseudo code as per below provided rules

            --- MANDATORY REFINEMENT RULES ---
            1. NO OMISSIONS: You must output the ENTIRE integrated pseudo-code. Do not use placeholders like "// ... (unchanged)" or "Logic remains the same." Every stage from the existing code must be present in the final output along with the new logic.
            2. LOGICAL MAPPING: Identify the object type (Transformer, Lookup, Connector, business logics, Hashed File) and insert it into the correct sequential flow (Source -> Processing -> Target).
            4. CLEANUP: Strip all UI-specific metadata (GUI coordinates, Canvas IDs, font settings, or color codes). Focus strictly on data transformation and movement.
            5. STABILITY: Ensure all previously defined stages and links are maintained to prevent "forgetting" logic from earlier iterations.
            6. Capture the descriptive overall flow (ex. connection building, datafetching, transformation, processing, cleaning, storage).
            7. Keep in mind to capture all the important steps (for transformation, ETL steps, validation steps, business logics, error handling steps etc) as thsi pseudo code will be used to convert it into other programming language like python, pyspark, snowpark etc.
            8. Focus mainly on logical part (which is important part of dataflow) not details of text or description of variables etc
            9. Add link in the pseudo code which makes flow clear previous and next link.
            10. Do not add any informal text of description apart from pseudo code.
            11. While capturing the details of columns definition. Only capture the technical metadata which is important for code conversion in python/pyspark/snowpark and details should be written in one line.
            12. If contains a SQL Query (Select, Insert, Update, or Join logic), do not decompose or analyze its internal logic. Pass it into the pseudo code as it is such that while converting pseudo code it will be executed as it is and output will be consumed in next links.
            13. Parent tag is defined for every chunk take note of the parent tags and use basic reasoning to map with other chunks accordingly also add those parent/child tag and input/output links in final pseudo code and preserve the parent/child heirarchy for every step.
            14. In the pseudo code after rearranging and updating the chunk code as per next chunk identify chunks of approximately less than 200 lines (apart from SQL query) and add placeholder between chunks for example : when psuedo code of chunk ends place "chunk pseudo code ended".
            15. Take all the columns in output which are deifned in pseudo code.
            
            \nEXISTING global pseudo code:\n {combined_chunks} 
            """
        
        start_time = time.time()
        
        # Keeping model switching logic exactly as provided
        if (index // 5) % 2 == 0:
            current_llm = llm_v4
            model_name = "Claude Sonnet (v4)"
        else:
            current_llm = llm_v4
            model_name = "Claude Sonnet (v4)"
        
        current_pseudo_code = current_llm.invoke(prompt).content.strip()
        duration = time.time() - start_time
        
        # Store result in output dictionary
        final_merged_output[output_key] = current_pseudo_code
        
        print(f"--- Processed Batch starting at chunk {batch_start+1} ---")
        print(f"Processing Time: {duration:.2f} seconds\n")
        print(f"--- Completed {output_key} --- Time: {duration:.2f}s ----- using model {model_name}")
    
        output_folder = "./batch_pseudo_prompt_final"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        filename = os.path.join((output_folder), f"{i}_batch_prompt_pseudo.xml")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(prompt)

        output_folder = "./batch_pseudo_code_final"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        filename = os.path.join((output_folder), f"{i}_batch_final_pseudo.xml")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(current_pseudo_code)
        
    
    return final_merged_output



# --- Keep existing LLM initializations exactly as provided ---

def generate_full_and_final_pseudo_code(chunks_dict):
    """
    Consumes dictionary output from the previous function.
    Keys are expected to be chunk_ids, values are XML/Pseudo strings.
    Iteratively merges all into one final master pseudo-code.
    """
    # Sort keys to ensure sequential merging (e.g., chunk_0_to_9, chunk_10_to_19)

    boto_config = Config(
    read_timeout=30000,
    connect_timeout=6000,
    retries={'max_attempts': 9, 'mode': 'adaptive'}
    )

    llm_v4 = ChatBedrock(
        model_id="..........",
        region_name=os.getenv("AWS_REGION", "....."),
        streaming=True,
        model_kwargs={"temperature": 0.0, "max_tokens": 65536},
        config=boto_config,
    )

    custom_timeout = httpx.Timeout(
        connect=30000.0, read=6000.0, write=6000.0, pool=6000.0
    )

    AZURE_INFERENCE_CREDENTIAL = "..................."
    AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = "g..........1"

    llm_v35 = AzureChatOpenAI(
        azure_endpoint=".........................",
        azure_deployment=AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,
        api_key=AZURE_INFERENCE_CREDENTIAL,
        api_version="....1",
        streaming=True,
        temperature=0.0,
        http_client=httpx.Client(timeout=custom_timeout)
    )


    chunk_keys = sorted(list(chunks_dict.keys()), key=lambda x: int(re.search(r"(\d+)", x).group()))
    
    if not chunk_keys:
        return ""

    # Initialize with the first chunk
    combined_chunks = chunks_dict[chunk_keys[0]]
    total_chunks = len(chunk_keys)
    
    print(f"Total batches to merge: {total_chunks}")
    
    # Iterate through remaining chunks to merge into the master 'combined_chunks'
    for i in range(1, total_chunks):
        chunk_id = chunk_keys[i]
        xml_content = chunks_dict[chunk_id]
        print(f"Merging: {chunk_id}")
        
        prompt = f"""Merge this NEW LOGIC of datastage XML pseudo code into the PRE-EXISTING pseudo code of datastage XML as per below provided rules

            --- MANDATORY REFINEMENT RULES ---
            1. NO OMISSIONS: You must output the ENTIRE integrated pseudo-code. Do not use placeholders like "// ... (unchanged)" or "Logic remains the same." Every stage from the existing code must be present in the final output along with the new logic.
            2. LOGICAL MAPPING: Identify the object type (Transformer, Lookup, Connector, Hashed File, business logics) and insert it into the correct sequential flow (Source -> Processing -> Target).
            3. CLEANUP: Strip all UI-specific metadata (GUI coordinates, Canvas IDs, font settings, or color codes). Focus strictly on data transformation and movement.
            4. STABILITY: Ensure all previously defined stages and links are maintained to prevent "forgetting" logic from earlier iterations.
            5. Capture the descriptive overall flow (ex. connection building, datafetching, transformation, processing, cleaning, storage).
            6. Keep in mind to capture all the important steps (for transformation, ETL steps, validation steps, businessc logic, error handling steps etc) as thsi pseudo code will be used to convert it into other programming language like python, pyspark, snowpark etc.
            7. Focus mainly on logical part (which is important part of dataflow) not details of text or description of variables etc
            8. Add link in the pseudo code which makes flow clear previous and next link.
            9. Do not add any informal text of description apart from pseudo code.
            10. While capturing the details of columns definition. Only capture the technical metadata which is important for code conversion in python/pyspark/snowpark and one line.
            11. If contains a SQL Query (Select, Insert, Update, or Join logic), do not decompose or analyze its internal logic. Pass it into the pseudo code as it is such that while converting pseudo code it will be executed as it is and output will be consumed in next links add a placeholder "SQL query found".
            12. Parent tag is defined for every chunk take note of the parent tags and use basic reasoning to map with other chunks accordingly also add those parent/child tag and input/output links in final pseudo code and preserve the parent/child heirarchy for every step.
            13. The generated pseudo code must be concise and use the minimum tokens necessary while remaining fully descriptive, logically complete, and compliant with all other rules. Avoid unnecessary verbosity but do not remove any steps, columns or logic.
            14. In the pseudo code after rearranging and updating the chunk code as per next chunk identify chunks of approximately less than 200 lines (apart from SQL query) and add placeholder between chunks for example : when psuedo code of chunk ends place "chunk pseudo code ended".
            
            \nEXISTING global pseudo code:\n {combined_chunks} \nNEW chunk of XML code pseudo code:\n {xml_content} and generate pseudo code adding this as it fits in the overall flow which is not more than 1200 lines(rearrange and update the overall flow)
            
            """

        start_time = time.time()
        # Keeping existing model switching logic
        if (i // 5) % 2 == 0:
            current_llm = llm_v4
            model_name = "Claude Sonnet (v4)"
        else:
            current_llm = llm_v4
            model_name = "Claude Sonnet (v4)"
            
        # Update global pseudo code with LLM output
        combined_chunks = current_llm.invoke(prompt).content.strip()
        
        duration = time.time() - start_time
        print(f"--- Processed {chunk_id} --- Time: {duration:.2f}s using {model_name}")

        output_folder = "./pseudo_prompt_final"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        filename = os.path.join((output_folder), f"{i}_prompt_pseudo.xml")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(prompt)

        output_folder = "./pseudo_code_final"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        filename = os.path.join((output_folder), f"{i}_final_pseudo.xml")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(combined_chunks)
        
        

    # Return only the final master pseudo-code
    return combined_chunks

# To use:
# final_master_pseudo_code = generate_workflow_logic(output_from_previous_function)

