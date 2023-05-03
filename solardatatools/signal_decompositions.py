# -*- coding: utf-8 -*-
""" Signal Decompositions Module

This module contains standardized signal decomposition models for use in the
SDT algorithms. The defined signal decompositions are:

1) 'l2_l1d1_l2d2p365': separating a piecewise constant component from a smooth
and seasonal component, with Gaussian noise
    - l2: gaussian noise, sum-of-squares small or l2-norm squared
    - l1d1: piecewise constant heuristic, l1-norm of first order differences
    - l2d2p365: small second order diffs (smooth) and 365-periodic
2) 'l1_l2d2p365': estimating a smooth, seasonal component with a laplacian
noise model, fitting a local median instead of a local average
    - l1: laplacian noise, sum-of-absolute values or l1-norm
    - l2d2p365: small second order diffs (smooth) and 365-periodic
3) 'tl1_l2d2p365': similar to (2), estimating a smooth, seasonal component with
an asymmetric laplacian noise model, fitting a local quantile instead of a
local average
    - tl1: 'tilted l1-norm,' also known as quantile cost function
    - l2d2p365: small second order diffs (smooth) and 365-periodic
4) 'tl1_l1d1_l2d2p365': like (1) but with an asymmetric residual cost instead
of Gaussian residuals
    - tl1: 'tilted l1-norm,' also known as quantile cost function
    - l1d1: piecewise constant heuristic, l1-norm of first order differences
    - l2d2p365: small second order diffs (smooth) and 365-periodic
5) 'hu_l1d1': total variation denoising with Huber residual cost
    - hu: Huber cost, a function that is quadratic below a cutoff point and
    linear above the cutoff point
    - l1d1: piecewise constant heuristic, l1-norm of first order differences

"""
import sys
import numpy as np
import cvxpy as cvx


def l2_l1d1_l2d2p365(
    signal,
    c0=10, # "hard-coded"
    c1=50, # optimized
    c2=1e4,
    solver=None,
    verbose=False,
    tv_weights=None,
    use_ixs=None,
    yearly_periodic=False,
    transition_locs=None
):
    """
    This performs total variation filtering with the addition of a seasonal
    baseline fit. This introduces a new signal to the model that is smooth and
    periodic on a yearly time frame. This does a better job of describing real,
    multi-year solar PV power data sets, and therefore does an improved job of
    estimating the discretely changing signal.

    :param signal: A 1d numpy array (must support boolean indexing) containing
    the signal of interest
    :param c1: The regularization parameter to control the total variation in
    the final output signal
    :param c2: The regularization parameter to control the smoothness of the
    seasonal signal
    :return: A 1d numpy array containing the filtered signal
    """
    if tv_weights is None:
        tv_weights = np.ones(len(signal) - 1)
    if use_ixs is None:
        index_set = ~np.isnan(signal)
    else:
        index_set = np.logical_and(use_ixs, ~np.isnan(signal))
    s_hat = cvx.Variable(len(signal))
    s_seas = cvx.Variable(len(signal))
    s_error = cvx.Variable(len(signal))
    c0 = cvx.Constant(value=c0)
    c1 = cvx.Constant(value=c1)
    c2 = cvx.Constant(value=c2)

    if transition_locs is None: # TODO: this should be two separate sd problems
        objective = cvx.Minimize(
            c0 * cvx.sum_squares(s_error)
            + c1 * cvx.norm1(cvx.multiply(tv_weights, cvx.diff(s_hat, k=1)))
            + c2 * cvx.sum_squares(cvx.diff(s_seas, k=2))
        )
    else:
        objective = cvx.Minimize(
            c0 * cvx.norm(s_error)
           + c2 * cvx.sum_squares(cvx.diff(s_seas, k=2))
        )
    # Consistency constraints
    constraints = [
        signal[index_set] == s_hat[index_set] + s_seas[index_set] + s_error[index_set],
        cvx.sum(s_seas[:365]) == 0,
    ]
    if len(signal) > 365:
        constraints.append(s_seas[365:] - s_seas[:-365] == 0)
        if yearly_periodic:
            constraints.append(s_hat[365:] - s_hat[:-365] == 0)
    if transition_locs is not None:
        loc_mask = np.ones(len(signal) - 1, dtype=bool)
        loc_mask[transition_locs] = False
        constraints.append(cvx.diff(s_hat, k=1)[loc_mask] == 0)

    problem = cvx.Problem(objective=objective, constraints=constraints)
    problem.solve(solver=solver, verbose=verbose)

    return s_hat.value, s_seas.value

def l1_l2d2p365( # need to remove this one, should no longer be used
        signal,
        use_ixs=None,
        c1=1e3,
        yearly_periodic=True, # default not overwritten in calls
        solver=None,
        verbose=False
):
    """
    for a list of available solvers, see:
        https://www.cvxpy.org/tutorial/advanced/index.html#solve-method-options

    :param signal: 1d numpy array
    :param use_ixs: optional index set to apply cost function to
    :param c1: float
    :param solver: string
    :return: median fit with seasonal baseline removed
    """
    if use_ixs is None:
        use_ixs = ~np.isnan(signal)
    else:
        use_ixs = np.logical_and(use_ixs, ~np.isnan(signal))

    x = cvx.Variable(len(signal))
    xr = cvx.Variable(len(signal))
    objective = cvx.Minimize(
       cvx.norm1(xr) + c1 * cvx.sum_squares(cvx.diff(x, k=2))
    )
    if len(signal) > 365 and yearly_periodic:
        constraints = [x[365:] == x[:-365]]
    else:
        constraints = []
    constraints.append(signal[use_ixs] == x[use_ixs] + xr[use_ixs])
    problem = cvx.Problem(objective, constraints=constraints)
    problem.solve(solver=solver, verbose=verbose)

    return x.value


def tl1_l2d2p365( # called 7 times
    signal,
    use_ixs=None,
    tau=0.75, # passed as 0.05 (sunrise), 0.95 (sunset), 0.9, 0.85
    c1=5e5,
    solver=None,
    yearly_periodic=True, # passed as False once
    verbose=False
):
    """
    https://colab.research.google.com/github/cvxgrp/cvx_short_course/blob/master/applications/quantile_regression.ipynb

    :param signal: 1d numpy array
    :param use_ixs: optional index set to apply cost function to
    :param tau: float, parameter for quantile regression
    :param c1: float
    :param solver: string
    :return: median fit with seasonal baseline removed
    """
    if use_ixs is None:
        use_ixs = ~np.isnan(signal)
    x = cvx.Variable(len(signal))
    r = signal[use_ixs] - x[use_ixs]
    objective = cvx.Minimize(
        cvx.sum(0.5 * cvx.abs(r) + (tau - 0.5) * r) + c1 * cvx.sum_squares(cvx.diff(x, k=2))
    )
    if len(signal) > 365 and yearly_periodic:
        constraints = [x[365:] == x[:-365]]
    else:
        constraints = []
    problem = cvx.Problem(objective, constraints=constraints)
    problem.solve(solver=solver, verbose=verbose)

    return x.value

def tl1_l1d1_l2d2p365( # called once
    signal,
    use_ixs=None,
    tau=0.995, # passed as 0.5
    c1=1e3, # passed as 15, l1d1 term
    c2=6000,
    c3=1e2, # passed as 300, linear term
    solver=None,
    verbose=False,
    tv_weights=None,
    linear_term=True
):
    """
    This performs total variation filtering with the addition of a seasonal baseline fit. This introduces a new
    signal to the model that is smooth and periodic on a yearly time frame. This does a better job of describing real,
    multi-year solar PV power data sets, and therefore does an improved job of estimating the discretely changing
    signal.

    :param signal: A 1d numpy array (must support boolean indexing) containing the signal of interest
    :param c1: The regularization parameter to control the total variation in the final output signal
    :param c2: The regularization parameter to control the smoothness of the seasonal signal
    :return: A 1d numpy array containing the filtered signal
    """
    n = len(signal)
    if tv_weights is None:
        tv_weights = np.ones(len(signal) - 1)
    # if use_ixs is None:
    #     use_ixs = np.ones(n, dtype=bool)
    if use_ixs is None:
        use_ixs = ~np.isnan(signal)
    else:
        use_ixs = np.logical_and(use_ixs, ~np.isnan(signal))
    # selected_days = np.arange(n)[index_set]
    # np.random.shuffle(selected_days)
    # ix = 2 * n // 3
    # train = selected_days[:ix]
    # validate = selected_days[ix:]
    # train.sort()
    # validate.sort()

    s_hat = cvx.Variable(n)
    s_seas = cvx.Variable(max(n, 366))
    s_error = cvx.Variable(n)
    c1 = cvx.Parameter(value=c1, nonneg=True)
    c2 = cvx.Parameter(value=c2, nonneg=True)
    c3 = cvx.Parameter(value=c3, nonneg=True)
    tau = cvx.Parameter(value=tau)
    beta = cvx.Variable()
    if linear_term:
        objective = cvx.Minimize(
            2
            * cvx.sum(
                0.5 * cvx.abs(s_error)
                + (tau - 0.5) * s_error
            )
            + c1 * cvx.norm1(cvx.multiply(tv_weights, cvx.diff(s_hat, k=1)))
            + c2 * cvx.sum_squares(cvx.diff(s_seas, k=2))
            + c3 * beta**2 # linear term that has a slope of beta over 1 year, done wrong
        )
    else:
        objective = cvx.Minimize(
            2
            * cvx.sum(
                0.5 * cvx.abs(s_error)
                + (tau - 0.5) * s_error
            )
            + c1 * cvx.norm1(cvx.multiply(tv_weights, cvx.diff(s_hat, k=1)))
            + c2 * cvx.sum_squares(cvx.diff(s_seas, k=2))
        )
    constraints = [
        signal[use_ixs] == s_hat[use_ixs] + s_seas[:n][use_ixs] + s_error[use_ixs],
        cvx.sum(s_seas[:365]) == 0,
    ]
    if linear_term:
        constraints.append(s_seas[365:] - s_seas[:-365] == beta)
        constraints.extend([beta <= 0.01, beta >= -0.1])
    problem = cvx.Problem(objective=objective, constraints=constraints)
    problem.solve(solver=solver, verbose=verbose)

    return s_hat.value, s_seas.value[:n]


def make_l2_l1d2_constrained(signal,
                 weight=1e1, # val ok
                 solver="MOSEK",
                 use_ixs=None
):
    """
    Used in solardatatools/algorithms/clipping.py
    Added hard-coded constraints on the first and last vals
    """
    if use_ixs is None:
        use_ixs = ~np.isnan(signal)
    else:
        use_ixs = np.logical_and(use_ixs, ~np.isnan(signal))

    y_hat = cvx.Variable(len(signal))
    mu = cvx.Parameter(nonneg=True)
    mu.value = weight
    error = cvx.sum_squares(signal[use_ixs] - y_hat[use_ixs])
    reg = cvx.norm(cvx.diff(y_hat, k=2), p=1)

    objective = cvx.Minimize(error + mu * reg)
    constraints = [y_hat[0] == 0, y_hat[-1] == 1]
    problem = cvx.Problem(objective, constraints)

    return problem, signal, y_hat, mu


##############################################################################
# NOT CURRENTLY USED
##############################################################################


# def hu_l1d1(signal, C=5):
#     """
#     This function performs total variation filtering or denoising on a 1D signal. This filter is implemented as a
#     convex optimization problem which is solved with cvxpy.
#     (https://en.wikipedia.org/wiki/Total_variation_denoising)
#
#     :param signal: A 1d numpy array (must support boolean indexing) containing the signal of interest
#     :param C: The regularization parameter to control the total variation in the final output signal
#     :return: A 1d numpy array containing the filtered signal
#     """
#     s_hat = cvx.Variable(len(signal))
#     mu = cvx.Constant(value=C)
#     index_set = ~np.isnan(signal)
#     objective = cvx.Minimize(
#         cvx.sum(cvx.huber(signal[index_set] - s_hat[index_set]))
#         + mu * cvx.norm1(cvx.diff(s_hat, k=1))
#     )
#     problem = cvx.Problem(objective=objective)
#     try:
#         problem.solve(solver="MOSEK")
#     except Exception as e:
#         print(e)
#         print("Trying ECOS solver")
#         problem.solve(solver="ECOS")
#     return s_hat.value
