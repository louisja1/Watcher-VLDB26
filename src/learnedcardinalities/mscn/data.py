import csv
import torch
from torch.utils.data import dataset

from learnedcardinalities.mscn.util import *


def load_data(
    training_set,
    table_template,
    join_template,
    predicate_template,
    num_materialized_samples,
):
    joins = []
    predicates = []
    tables = []
    samples = []
    label = []

    # Load queries
    for item in training_set:
        bounds = item[:-1]
        card = item[-1]
        if card < 1:
            continue
        tables.append(table_template.copy())
        joins.append(join_template.copy())
        cur_predicate = predicate_template.copy()
        for i in range(len(bounds)):
            adj_tmp = -1 + 2 * (i % 2)
            # i is even (lower bound), should -1; otherwise i is odd (upper bound), should + 1;
            cur_predicate[2 + i * 3] = str(bounds[i] + adj_tmp)

        # for i in range(len(table_template)):
        #     abbrev = table_template[i].strip().split(" ")[1]
        #     cur_predicate.extend(
        #         [f"{abbrev}.{abbrev}_item_sk", ">", str(bounds[i * 2] - 1)]
        #     )
        #     cur_predicate.extend(
        #         [f"{abbrev}.{abbrev}_item_sk", "<", str(bounds[i * 2 + 1] + 1)]
        #     )
        predicates.append(cur_predicate.copy())
        label.append(str(card))
    # print("Loaded queries")

    # Split predicates
    predicates = [list(chunks(d, 3)) for d in predicates]

    return joins, predicates, tables, samples, label


def load_and_encode_train_data(
    training_set,
    table_template,
    join_template,
    column_min_max_vals,
    predicate_template,
    num_queries,
    num_materialized_samples,
):
    joins, predicates, tables, samples, label = load_data(
        training_set,
        table_template,
        join_template,
        predicate_template,
        num_materialized_samples,
    )

    if len(label) <= int(1500 * 0.9):
        num_queries = len(label)

    # Get column name dict
    column_names = get_all_column_names(predicates)
    column2vec, idx2column = get_set_encoding(column_names)

    # Get table name dict
    table_names = get_all_table_names(tables)
    table2vec, idx2table = get_set_encoding(table_names)

    # Get operator name dict
    operators = get_all_operators(predicates)
    op2vec, idx2op = get_set_encoding(operators)

    # Get join name dict
    join_set = get_all_joins(joins)
    join2vec, idx2join = get_set_encoding(join_set)

    # Get min and max values for each column

    # Get feature encoding and proper normalization
    samples_enc = encode_samples(tables, samples, table2vec)
    predicates_enc, joins_enc = encode_data(
        predicates, joins, column_min_max_vals, column2vec, op2vec, join2vec
    )
    label_norm, min_val, max_val = normalize_labels(label)

    # Split in training and validation samples
    num_train = int(num_queries * 0.9)
    num_test = num_queries - num_train

    samples_train = samples_enc[:num_train]
    predicates_train = predicates_enc[:num_train]
    joins_train = joins_enc[:num_train]
    labels_train = label_norm[:num_train]

    samples_test = samples_enc[num_train : num_train + num_test]
    predicates_test = predicates_enc[num_train : num_train + num_test]
    joins_test = joins_enc[num_train : num_train + num_test]
    labels_test = label_norm[num_train : num_train + num_test]

    # print("Number of training samples: {}".format(len(labels_train)))
    # print("Number of validation samples: {}".format(len(labels_test)))

    max_num_joins = max(
        max([len(j) for j in joins_train]), max([len(j) for j in joins_test])
    )
    max_num_predicates = max(
        max([len(p) for p in predicates_train]), max([len(p) for p in predicates_test])
    )

    dicts = [table2vec, column2vec, op2vec, join2vec]
    train_data = [samples_train, predicates_train, joins_train]
    test_data = [samples_test, predicates_test, joins_test]
    return (
        dicts,
        min_val,
        max_val,
        labels_train,
        labels_test,
        max_num_joins,
        max_num_predicates,
        train_data,
        test_data,
    )


def make_dataset(samples, predicates, joins, labels, max_num_joins, max_num_predicates):
    """Add zero-padding and wrap as tensor dataset."""

    sample_masks = []
    sample_tensors = []
    for sample in samples:
        sample_tensor = np.vstack(sample)
        num_pad = max_num_joins + 1 - sample_tensor.shape[0]
        sample_mask = np.ones_like(sample_tensor).mean(1, keepdims=True)
        sample_tensor = np.pad(sample_tensor, ((0, num_pad), (0, 0)), "constant")
        sample_mask = np.pad(sample_mask, ((0, num_pad), (0, 0)), "constant")
        sample_tensors.append(np.expand_dims(sample_tensor, 0))
        sample_masks.append(np.expand_dims(sample_mask, 0))
    sample_tensors = np.vstack(sample_tensors)
    sample_tensors = torch.FloatTensor(sample_tensors)
    sample_masks = np.vstack(sample_masks)
    sample_masks = torch.FloatTensor(sample_masks)

    predicate_masks = []
    predicate_tensors = []
    for predicate in predicates:
        predicate_tensor = np.vstack(predicate)
        num_pad = max_num_predicates - predicate_tensor.shape[0]
        predicate_mask = np.ones_like(predicate_tensor).mean(1, keepdims=True)
        predicate_tensor = np.pad(predicate_tensor, ((0, num_pad), (0, 0)), "constant")
        predicate_mask = np.pad(predicate_mask, ((0, num_pad), (0, 0)), "constant")
        predicate_tensors.append(np.expand_dims(predicate_tensor, 0))
        predicate_masks.append(np.expand_dims(predicate_mask, 0))
    predicate_tensors = np.vstack(predicate_tensors)
    predicate_tensors = torch.FloatTensor(predicate_tensors)
    predicate_masks = np.vstack(predicate_masks)
    predicate_masks = torch.FloatTensor(predicate_masks)

    join_masks = []
    join_tensors = []
    for join in joins:
        join_tensor = np.vstack(join)
        num_pad = max_num_joins - join_tensor.shape[0]
        join_mask = np.ones_like(join_tensor).mean(1, keepdims=True)
        join_tensor = np.pad(join_tensor, ((0, num_pad), (0, 0)), "constant")
        join_mask = np.pad(join_mask, ((0, num_pad), (0, 0)), "constant")
        join_tensors.append(np.expand_dims(join_tensor, 0))
        join_masks.append(np.expand_dims(join_mask, 0))
    join_tensors = np.vstack(join_tensors)
    join_tensors = torch.FloatTensor(join_tensors)
    join_masks = np.vstack(join_masks)
    join_masks = torch.FloatTensor(join_masks)

    target_tensor = torch.FloatTensor(labels)

    return dataset.TensorDataset(
        sample_tensors,
        predicate_tensors,
        join_tensors,
        target_tensor,
        sample_masks,
        predicate_masks,
        join_masks,
    )


def get_train_datasets(
    training_set,
    table_template,
    join_template,
    column_min_max_vals,
    predicate_template,
    num_queries,
    num_materialized_samples,
):
    (
        dicts,
        min_val,
        max_val,
        labels_train,
        labels_test,
        max_num_joins,
        max_num_predicates,
        train_data,
        test_data,
    ) = load_and_encode_train_data(
        training_set,
        table_template,
        join_template,
        column_min_max_vals,
        predicate_template,
        num_queries,
        num_materialized_samples,
    )
    train_dataset = make_dataset(
        *train_data,
        labels=labels_train,
        max_num_joins=max_num_joins,
        max_num_predicates=max_num_predicates,
    )
    # print("Created TensorDataset for training data")
    test_dataset = make_dataset(
        *test_data,
        labels=labels_test,
        max_num_joins=max_num_joins,
        max_num_predicates=max_num_predicates,
    )
    # print("Created TensorDataset for validation data")
    return (
        dicts,
        min_val,
        max_val,
        labels_train,
        labels_test,
        max_num_joins,
        max_num_predicates,
        train_dataset,
        test_dataset,
    )
