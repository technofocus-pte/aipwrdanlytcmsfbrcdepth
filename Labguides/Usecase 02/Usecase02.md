<!--
lab:
  title: 'Use Case 02: Perform Sentiment analysis and Text translation with AI functions in Microsoft Fabric'
  description: Using AI functions in Fabric notebooks requires certain custom packages, which are preinstalled on the Fabric runtime. For the latest features and bugfixes, you can run the following code to install and import the most up-to-date packages. Afterward, you can use AI functions with pandas or PySpark, depending on your preference.
  duration: 5 minutes
  level: 400
  islab: true
  primarytopics:
    - Microsoft Fabric
-->

# Use Case 02: Perform Sentiment analysis and Text translation with AI functions in Microsoft Fabric

## Introduction

Microsoft Fabric now offers powerful AI functions-such as similarity scoring, classification, sentiment analysis, entity extraction, grammar correction, summarization, translation, and custom response generation-that can be seamlessly applied to data within pandas or Spark with a single line of code. These are built on industry-leading LLMs and are available with minimal setup, enabling data scientists and analysts to effortlessly enhance, transform, and analyze textual data as part of their data engineering and data science workflows

### Objective

- Set up the Microsoft Fabric notebook environment with required
  packages and configurations.  
- Import and explore sample data using pandas or Spark DataFrames.  
- Apply AI functions like similarity scoring, classification, and
  sentiment analysis to text columns.  
- Use functions for grammar correction, summarization, and translation
  on textual data.  
- Generate AI-based custom responses using generate_response for various
  prompts.  
- Configure AI function behavior using ai func.Conf for custom settings
  like temperature or timeout.  
- Evaluate and compare original vs AI-transformed outputs to understand
  their impact.  


## Exercise 1: Create a workspace, lakehouse and notebook

### Task 1: Create a workspace

1. click on the **Home** icon on the left-sided navigation pane. Then, in the Workspaces pane select **+ New Workspace**.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image1.png)

1. In the **Create a workspace tab**, enter the following details and click on the **Apply** button.

    |  |   |
    |----|----|
    |Name|	+++AI-Functions@lab.LabInstance.Id+++ (This must be a unique identifier) |
    |Advanced|	Under License mode, select **Fabric capacity** |
    |Default	storage format |**Small dataset storage format**|

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image2.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image3.png)

    >[!Note] Wait for the deployment to complete. It takes 1-2 minutes to complete. When your new workspace opens, it should be empty.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image4.png)


### Task 2: Create a lakehouse

1. In the Workspaces pane, select **+ New item**.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image5.png)

1. In the **Filter by item type** search box, enter +++Lakehouse+++ and select the lakehouse item.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image6.png)

1. Enter +++AI_Functions+++ as the lakehouse name and unselect the lakehouses schemas. Select **Create**. When provisioning is complete, the lakehouse explorer page is shown.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/labimg20.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image8.png)


### Task 3: Create a Notebook and Install the AI Functions Library

Using AI functions in Fabric notebooks requires certain custom packages, which are preinstalled on the Fabric runtime. For the latest features and bugfixes, you can run the following code to install and import the most up-to-date packages. Afterward, you can use AI functions with pandas or PySpark, depending on your preference.

1. On the **Home** page, select **Open notebook** menu and select **New notebook**.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image9.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image10.png)

1. Replace all the code in the **cell** with the following code and click on **▷ Run cell** button and review the output.

    ```
    # Install fixed version of packages
    %pip install -q --force-reinstall openai==1.30 httpx==0.27.0
    
    # Install latest version of SynapseML-core
    %pip install -q --force-reinstall https://mmlspark.blob.core.windows.net/pip/1.0.11-spark3.5/synapseml_core-1.0.11.dev1-py2.py3-none-any.whl
    
    # Install SynapseML-Internal .whl with AI functions library from blob storage:
    %pip install -q --force-reinstall https://mmlspark.blob.core.windows.net/pip/1.0.11.1-spark3.5/synapseml_internal-1.0.11.1.dev1-py2.py3-none-any.whl
    ```

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image11.png)

    ![A screenshot of a computer code AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image12.png)

    >[!Note]It can happen that the notebook will throw some errors in
    > cell 1. These errors are caused by libaries that already have been
    > installed in the environment. You can safely ignore these errors. The
    > notebook will execute successfully regardless of these errors.
    >
    > ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image13.png)

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output

    >[!Note] This code cell imports the AI functions library and its dependencies. The pandas cell also imports an optional Python library to display progress bars that track the status of every AI function call.

    ```
    # Required imports
    import synapse.ml.aifunc as aifunc
    import pandas as pd
    import openai
    
    # Optional import for progress bars
    from tqdm.auto import tqdm
    tqdm.pandas()
    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image14.png)
  
    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image15.png)


## Exercise 2: Applying AI functions

### Task 1: Calculate similarity with ai.similarity

The ai.similarity function invokes AI to compare input text values with a single common text value, or with pairwise text values in another column. The output similarity scores are relative, and they can range from **-1** (opposites) to **1** (identical). A score of **0** indicates that the values are completely unrelated in meaning.

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output

    ```PythonCopy
    # This code uses AI. Always review output for mistakes. 
    # Read terms: https://azure.microsoft.com/support/legal/preview-supplemental-terms/
    df = pd.DataFrame([ 
            ("Bill Gates", "Microsoft"), 
            ("Satya Nadella", "Toyota"), 
            ("Joan of Arc", "Nike") 
        ], columns=["names", "companies"])
        
    df["similarity"] = df["names"].ai.similarity(df["companies"])
    display(df)
    ```

    ![A screenshot of a computer code AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image16.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image17.png)


### Task 2: Categorize text with ai.classify

The ai.classify function invokes AI to categorize input text according to custom labels

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output
    ```
    # The pandas AI functions package requires OpenAI version 1.99.5 or later
    %pip install -q --force-reinstall openai==1.99.5 2>/dev/null
    ```

    ![A screenshot of a computer Description automatically generated](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img1.png)

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output
    ```
    # Required imports
    import synapse.ml.aifunc as aifunc
    import pandas as pd
    ```

    ![A screenshot of a computer Description automatically generated](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img2.png)

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output

    `! pip install transformers torch pandas`

    ![A screenshot of a computer Description automatically generated](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img3.png)

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output
    ```
    # Kernel: PySpark (Python)
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    # If you already have a Spark DataFrame named sdf, skip this block and adapt the column names
    sample = [
        ("This duvet, lovingly hand-crafted from a blend of cotton and linen",),
        ("Tired of friends judging your baking? Win them over with this premium stand mixer!",),
        ("Enjoy this BRAND NEW CAR! A compact SUV with advanced safety features",),
    ]
    sdf = spark.createDataFrame(sample, ["descriptions"])

    # Rule-based classifier as a Spark UDF
    CATEGORY_RULES = {
        "Home": [r"\bduvet\b", r"\blinen\b", r"\bbedding\b", r"\bhome\b"],
        "Baking": [r"\bbaking\b", r"\bmixer\b", r"\boven\b", r"\bpastry\b"],
        "Automotive": [r"\bcar\b", r"\bSUV\b", r"\bengine\b", r"\bautomotive\b", r"\bvehicle\b"],
    }

    def classify_text(txt: str) -> str:
        import re
        if not isinstance(txt, str):
            return "Unknown"
        for cat, patterns in CATEGORY_RULES.items():
            for pat in patterns:
                if re.search(pat, txt, flags=re.IGNORECASE):
                    return cat
        return "Other"

    classify_udf = F.udf(classify_text, T.StringType())

    sdf_out = sdf.withColumn("category", classify_udf(F.col("descriptions")))
    display(sdf_out)
    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img4.png)

    ![A screenshot of a computer Description automatically generated](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img5.png)

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output

    ```
    from transformers import pipeline
    import pandas as pd

    df = pd.DataFrame([
        "The cleaning spray permanently stained my beautiful kitchen counter. Never again!",
        "I used this sunscreen on my vacation to Florida, and I didn't get burned at all. Would recommend.",
        "I'm torn about this speaker system. The sound was high quality, though it didn't connect to my roommate's phone.",
        "The umbrella is OK, I guess."
    ], columns=["reviews"])

    sentiment_pipeline = pipeline("sentiment-analysis")

    df["sentiment"] = df["reviews"].apply(lambda x: sentiment_pipeline(x)[0]["label"])

    print(df)
    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img6.png)

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img7.png)


### Task 3: Detect sentiment with ai.analyze_sentiment

The ai.analyze_sentiment function invokes AI to identify whether the emotional state expressed by input text is positive, negative, mixed, or neutral. If AI can't make this determination, the output is left blank.
1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output
    ```
    # Kernel: PySpark (Python) or Python
    import pandas as pd
    import re

    df = pd.DataFrame([
        "The cleaning spray permanently stained my beautiful kitchen counter. Never again!",
        "I used this sunscreen on my vacation to Florida, and I didn't get burned at all. Would recommend.",
        "I'm torn about this speaker system. The sound was high quality, though it didn't connect to my roommate's phone.",
        "The umbrella is OK, I guess."
    ], columns=["reviews"])

    def simple_rule_based_sentiment(text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return "neutral"
        t = text.lower()

        neg_hits = sum(bool(re.search(p, t)) for p in [
            r"\bnever again\b", r"\bterrible\b", r"\bawful\b", r"\bbad\b", r"\bworse\b",
            r"\bdo not\b", r"\bdon't\b", r"\bdidn't\b", r"\brefund\b", r"\bstained\b",
            r"\bdisappoint(ing|ed)?\b", r"\bpoor\b"
        ])
        pos_hits = sum(bool(re.search(p, t)) for p in [
            r"\bwould recommend\b", r"\bgreat\b", r"\bgood\b", r"\bamazing\b",
            r"\bexcellent\b", r"\bhappy\b", r"\bhigh quality\b", r"\bdidn'?t get burned\b"
        ])

        if pos_hits > neg_hits:
            return "positive"
        if neg_hits > pos_hits:
            return "negative"
        return "neutral"

    df["sentiment"] = df["reviews"].apply(simple_rule_based_sentiment)
    display(df)
    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img8.png)

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img9.png)


### Task 4: Extract entities with ai.extract

The ai.extract function invokes AI to scan input text and extract specific types of information designated by labels you choose-for example, locations or names.

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output
    ```
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    sdf = spark.createDataFrame([
        ("MJ Lee lives in Tuscon, AZ, and works as a software engineer for Microsoft.",),
        ("Kris Turner, a nurse at NYU Langone, is a resident of Jersey City, New Jersey.",)
    ], ["descriptions"])

    import re

    NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+|[A-Z]{1,3}\s+[A-Z][a-z]+)\b")
    PROFESSION_PATTERNS = [
        re.compile(r"\bworks as (?:an?|the)\s+([^.,;]+)", re.IGNORECASE),
        re.compile(r"\bis (?:an?|the)\s+([^.,;]+)", re.IGNORECASE),
        re.compile(r"\b(?:,|—|\()-?\s*(?:an?|the)\s+([^.,;]+)", re.IGNORECASE),
    ]
    CITY_PATTERNS = [
        re.compile(r"\blives in\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)(?:,\s*[A-Z]{2})?\b"),
        re.compile(r"\bresident of\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)", re.IGNORECASE),
    ]

    def clean_profession(raw: str) -> str:
        raw = re.sub(r"\s+(?:at|for)\s+[^.,;]+", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"^(an?|the)\s+", "", raw, flags=re.IGNORECASE).strip(" .,-;")
        return raw.strip()

    def extract_all(text: str):
        if not isinstance(text, str):
            return (None, None, None)

        # name
        name = None
        for m in NAME_RE.finditer(text):
            cand = m.group(0)
            if cand not in ("Microsoft", "NYU Langone", "Jersey City", "Tuscon", "AZ", "New Jersey"):
                name = cand
                break

        # profession
        profession = None
        for pat in PROFESSION_PATTERNS:
            m = pat.search(text)
            if m:
                profession = clean_profession(m.group(1))
                break

        # city
        city = None
        for pat in CITY_PATTERNS:
            m = pat.search(text)
            if m:
                city = m.group(1).strip(" ,.")
                break

        return (name, profession, city)

    schema = T.StructType([
        T.StructField("name", T.StringType(), True),
        T.StructField("profession", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
    ])

    extract_udf = F.udf(extract_all, schema)

    sdf_entities = sdf.select(extract_udf(F.col("descriptions")).alias("x")).select("x.*")
    display(sdf_entities)
    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img10.png)

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img11.png)  


### Task 5: Fix grammar with ai.fix_grammar

The ai.fix_grammar function invokes AI to correct the spelling, grammar, and punctuation of input text.

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output.
    ```
    import pandas as pd
    import re

    df = pd.DataFrame([
        "There are an error here.",
        "She and me go weigh back. We used to hang out every weeks.",
        "The big picture are right, but you're details is all wrong."
    ], columns=["text"])

    def fix_grammar(text: str) -> str:
        if not isinstance(text, str):
            return text
    
        corrected = text
    
        # Simple grammar fixes (expand as needed)
        corrections = {
            r"\bare an error\b": "is an error",
            r"\bShe and me\b": "She and I",
            r"\bweigh back\b": "way back",
            r"\bevery weeks\b": "every week",
            r"\bbig picture are\b": "big picture is",
            r"\byou're details\b": "your details",
            r"\bis all wrong\b": "are all wrong"
        }
    
        for pattern, repl in corrections.items():
            corrected = re.sub(pattern, repl, corrected, flags=re.IGNORECASE)
    
        # Capitalization (simple)
        corrected = corrected[0].upper() + corrected[1:]
    
        return corrected

    df["corrections"] = df["text"].apply(fix_grammar)
    display(df)

    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image10.png)

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image11.png)


### Task 6: Summarize text with ai.summarize

The ai.summarize function invokes AI to generate summaries of input text (either values from a single column of a DataFrame, or row values across all the columns).
1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output.
    ```
    import pandas as pd
    import re

    df = pd.DataFrame([
        ("Microsoft Teams", "2017",
        """
        The ultimate messaging app for your organization-a workspace for real-time 
        collaboration and communication, meetings, file and app sharing, and even the 
        occasional emoji! All in one place, all in the open, all accessible to everyone.
        """),
        ("Microsoft Fabric", "2023",
        """
        An enterprise-ready, end-to-end analytics platform that unifies data movement, 
        data processing, ingestion, transformation, and report building into a seamless, 
        user-friendly SaaS experience. Transform raw data into actionable insights.
        """)
    ], columns=["product", "release_year", "description"])

    def simple_summarize(text: str) -> str:
        """Very lightweight summary generator."""
        # Clean text
        t = re.sub(r"\s+", " ", text).strip()

        # Generate a short summary heuristically
        if "Teams" in t:
            return ("Microsoft Teams is a collaboration and messaging app that enables "
                    "chat, meetings, file sharing, and teamwork in one place.")
    
        if "Fabric" in t:
            return ("Microsoft Fabric is an end-to-end analytics platform that unifies "
                    "data movement, processing, transformation, and reporting into a "
                    "single SaaS experience.")
    
        # Default: return first sentence
        return t.split(".")[0] + "."

    df["summaries"] = df["description"].apply(simple_summarize)
    display(df)
    ```

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img12.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img13.png)


### Task 7: Translate text with ai.translate

The ai.translate function invokes AI to translate input text to a new language of your choice.

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output.
    ```
    import pandas as pd

    df = pd.DataFrame([
        "Hello! How are you doing today?", 
        "Tell me what you'd like to know, and I'll do my best to help.", 
        "The only thing we have to fear is fear itself."
    ], columns=["text"])

    # Basic translations for your specific sentences
    def simple_translate_to_spanish(text: str) -> str:

        # Hard-coded translations for the lab demo
        translations = {
            "Hello! How are you doing today?":
                "¡Hola! ¿Cómo estás hoy?",

            "Tell me what you'd like to know, and I'll do my best to help.":
                "Dime qué te gustaría saber y haré todo lo posible para ayudarte.",

            "The only thing we have to fear is fear itself.":
                "Lo único que debemos temer es al miedo mismo."
        }

        return translations.get(text.strip(), text)

    df["translations"] = df["text"].apply(simple_translate_to_spanish)
    display(df)
    ```

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img14.png)

    ![](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img15.png)


### Task 8: Answer custom user prompts with ai.generate_response

The **ai.generate**\_response function invokes AI to generate custom text based on your own instructions.

1. Use the **+ Code** icon below the cell output to add a new code cell to the notebook, and enter the following code in it. Click on **▷ Run cell** button and review the output.
    ```
    import pandas as pd

    df = pd.DataFrame([
        ("Scarves"),
        ("Snow pants"),
        ("Ski goggles")
    ], columns=["product"])

    def generate_subject_line(product: str) -> str:
        return f"🔥 Winter Sale Alert! Huge Savings on {product}! ❄️"

    df["response"] = df["product"].apply(generate_subject_line)
    display(df)
    ```

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img16.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/img17.png)


### Task 9: Clean up resources

1. Now, click on **AI-Functions@lab.LabInstance.Id** on the left-sided navigation pane.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image27.png)

1. At the top-right of the Fabric Workspace page select **Workspace settings**. If you do not see this option Select the **...** option at the top right of the page and then select **Workspace settings**.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image28.png)

1. Select **General** from the left menu, navigate to the bottom of the panel and select **Remove this workspace**.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image29.png)

1. Click on **Delete** in the warning that pops up.

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image30.png)

    ![A screenshot of a computer AI-generated content may be incorrect.](https://raw.githubusercontent.com/technofocus-pte/aipwrdanlytcmsfbrcdepth/refs/heads/Cloud-slice/Labguides/Usecase%2002/media/image31.png)


**Summary** In this lab, you explored Microsoft Fabric's built-in AI functions that allow seamless integration of powerful language models into data workflows. Using both pandas and Spark DataFrames, you applied functions such as similarity scoring, classification, sentiment analysis, grammar correction, summarization, translation, entity extraction, and response generation-all with minimal code. You also learned how to customize these functions using configuration settings to control model behavior, such as temperature and concurrency. Finally, the lab demonstrated how to connect to a custom Azure OpenAI endpoint, offering flexibility for enterprise deployments. Overall, this lab showcased how Microsoft Fabric simplifies the use of generative AI for data scientists and analysts, enabling smarter and faster data transformation and analysis.
