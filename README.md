# processing-large-csv-data

_DRAFT_

This script is meant to work inside the current Deephaven IDE.  Pleasee see our [Quickstart](https://deephaven.io/core/docs/tutorials/quickstart/) before running commands from `process.py`

Recently [Conor O'Sullivan](https://conorosullyds.medium.com/) wrote a great article on [Batch Processing 22GB of Transaction Data with Pandas](https://towardsdatascience.com/batch-processing-22gb-of-transaction-data-with-pandas-c6267e65ff36) which discusses "How you get around limited computational resources and work with large datasets." The data set is a single CSV file of 22GB. Here is the full dataset on [Kaggle](https://www.kaggle.com/conorsully1/simulated-transactions). You can also find the notebook [Connor's tutorial on GitHub](https://github.com/conorosully/medium-articles/blob/master/src/batch_processing.ipynb) or the [Deephaven example on GitHub](https://github.com/deephaven-examples/processing-large-csv-data).

Using Pandas with limited resources Connor noted aggregations took about 50 minutes each.  

In this example, I'll show you how to take the Panads example, also with limited resources, and use [Deephaven](https://deephaven.io/) to speed things up as much as possible.

The first issue with this data set is loading the data to work with in Python.  Using Pandas on the full dataset poses a problem as Pandas tries to load the entire data set into memory.  With limited resources this is not possible and causes kernal to die.

The Deephaven approach to csv files is a little different. For more information see our [blog post on CSV](https://deephaven.io/blog/2022/02/23/csv-reader/).


When you run this code, note that the time for a Panda aggregation [Conor O'Sullivan](https://conorosullyds.medium.com/) notes takes about 50 minutes. On my laptop the time is about 90 minutes. While the Deephaven aggregation, since we are built for large data, takes less than one second.  

Time tests are wrapping every method. Comment out what operation you want to test to see its performance. Let us know how your query does on [Slack](https://join.slack.com/t/deephavencommunity/shared_invite/zt-11x3hiufp-DmOMWDAvXv_pNDUlVkagLQ).
