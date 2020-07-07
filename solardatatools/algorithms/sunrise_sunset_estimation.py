''' Sunrise Sunset Estimation Algorithm Module

This module contains an algorithm for robust estimation of sunrise and sunset
times from measured power data. This algorithm utilizes the following prior
information:

 - That sunrise and sunset times vary slowly from day to day (smooth signals)
 - That sunrise and sunset times repeat on a yearly basis (periodic signals)

This algorithm attempt to estimate the true sunrise and sunset times, in
accordance with standard geometric models of sun position. It uses holdout
validation to tune the theshold parameter, as opposed to the subroutines in
the DataHandler pipeline which do not tune this parameter. This algorithm
should be used in situations where exact estimates of true sunrise and sunset
times are required, such as for latitude and longitude estimation from data.

Bennet Meyers, 7/2/20

'''

import numpy as np
from solardatatools.daytime import detect_sun
from solardatatools.sunrise_sunset import rise_set_rough, rise_set_smoothed
from solardatatools.utilities import local_quantile_regression_with_seasonal

class SunriseSunset_v2():
    def __init__(self):
        self.sunrise_estimates = None
        self.sunset_estimates = None
        self.sunrise_measurements = None
        self.sunset_measurements = None
        self.sunup_mask = None
        self.threshold = None

    def run(self, data, random_seed=None):
        ths = np.logspace(-5, -1, 31)
        ho_error = []
        for th in ths:
            bool_msk = detect_sun(data, th)
            measured = rise_set_rough(bool_msk)
            sunrises = measured['sunrises']
            sunsets = measured['sunsets']
            # np.random.seed(random_seed)
            use_set_sr = np.arange(len(sunrises))[~np.isnan(sunrises)]
            use_set_ss = np.arange(len(sunsets))[~np.isnan(sunsets)]
            if len(use_set_sr) / len(sunrises) > 0.6 and len(use_set_ss) / len(sunsets) > 0.6:
                selected_th = th
                break
            else:
                selected_th = None
            #     np.random.shuffle(use_set_sr)
            #     np.random.shuffle(use_set_ss)
            #     split_at_sr = int(len(use_set_sr) * .8)     # 80-20 train test split
            #     split_at_ss = int(len(use_set_ss) * .8)
            #     train_sr = use_set_sr[:split_at_sr]
            #     train_ss = use_set_ss[:split_at_ss]
            #     test_sr = use_set_sr[split_at_sr:]
            #     test_ss = use_set_ss[split_at_ss:]
            #     train_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
            #     train_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
            #     train_msk_sr[train_sr] = True
            #     train_msk_ss[train_ss] = True
            #     test_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
            #     test_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
            #     test_msk_sr[test_sr] = True
            #     test_msk_ss[test_ss] = True
            #     sr_smoothed = local_quantile_regression_with_seasonal(sunrises,
            #                                                           train_msk_sr,
            #                                                           tau=0.05,
            #                                                           solver='MOSEK')
            #     ss_smoothed = local_quantile_regression_with_seasonal(sunsets,
            #                                                           train_msk_ss,
            #                                                           tau=0.95,
            #                                                           solver='MOSEK')
            #     r1 = (sunrises - sr_smoothed)[test_msk_sr]
            #     r2 = (sunsets - ss_smoothed)[test_msk_ss]
            #     ho_resid = np.r_[r1, r2]
            #     ho_error.append(np.sqrt(np.mean(ho_resid ** 2)))
            # else:
            #     ho_error.append(1e6)
            # selected_th = ths[np.argmin(ho_error)]
        bool_msk = detect_sun(data, selected_th)
        measured = rise_set_rough(bool_msk)
        smoothed = rise_set_smoothed(measured, sunrise_tau=.05,
                                     sunset_tau=.95)
        self.sunrise_estimates = smoothed['sunrises']
        self.sunset_estimates = smoothed['sunsets']
        self.sunrise_measurements = measured['sunrises']
        self.sunset_measurements = measured['sunsets']
        self.sunup_mask = bool_msk
        self.threshold = selected_th

''' Sunrise Sunset Estimation Algorithm Module

This module contains an algorithm for robust estimation of sunrise and sunset
times from measured power data. This algorithm utilizes the following prior
information:

 - That sunrise and sunset times vary slowly from day to day (smooth signals)
 - That sunrise and sunset times repeat on a yearly basis (periodic signals)

This algorithm attempt to estimate the true sunrise and sunset times, in
accordance with standard geometric models of sun position. It uses holdout
validation to tune the theshold parameter, as opposed to the subroutines in
the DataHandler pipeline which do not tune this parameter. This algorithm
should be used in situations where exact estimates of true sunrise and sunset
times are required, such as for latitude and longitude estimation from data.

Bennet Meyers, 7/2/20

'''

import numpy as np
import matplotlib.pyplot as plt
from solardatatools.daytime import detect_sun
from solardatatools.sunrise_sunset import rise_set_rough, rise_set_smoothed
from solardatatools.utilities import local_quantile_regression_with_seasonal


class SunriseSunset():
    def __init__(self):
        self.sunrise_estimates = None
        self.sunset_estimates = None
        self.sunrise_measurements = None
        self.sunset_measurements = None
        self.sunup_mask = None
        self.threshold = None
        self.total_rmse = None

    def calculate_times(self, data, threshold=None, plot=False,
                        figsize=(12, 10), groundtruth=None, zoom_fit=False):
        if threshold is None:
            if self.threshold is not None:
                threshold = self.threshold
            else:
                print('Please run optimizer or provide a threshold')
                return
        if groundtruth is not None:
            sr_true = groundtruth[0]
            ss_true = groundtruth[1]
        else:
            sr_true = None
            ss_true = None
        bool_msk = detect_sun(data, threshold)
        measured = rise_set_rough(bool_msk)
        smoothed = rise_set_smoothed(measured, sunrise_tau=.05,
                                     sunset_tau=.95)
        self.sunrise_estimates = smoothed['sunrises']
        self.sunset_estimates = smoothed['sunsets']
        self.sunrise_measurements = measured['sunrises']
        self.sunset_measurements = measured['sunsets']
        self.sunup_mask = bool_msk
        self.threshold = threshold

        if plot:
            fig, ax = plt.subplots(nrows=4, figsize=figsize)
            ylims = []
            ax[0].set_title('Sunrise Times')
            ax[0].plot(self.sunrise_estimates, label='estimated', ls='--')
            ylims.append(ax[0].get_ylim())
            ax[0].plot(self.sunrise_measurements, label='measured', marker='.',
                       ls='none', alpha=0.3, color='green')
            if groundtruth is not None:
                ax[0].plot(sr_true, label='true', color='orange')
            ax[1].set_title('Sunset Times')
            ax[1].plot(self.sunset_estimates, label='estimated', ls='--')
            ylims.append(ax[1].get_ylim())
            ax[1].plot(self.sunset_measurements, label='measured', marker='.',
                       ls='none', alpha=0.3, color='green')
            if groundtruth is not None:
                ax[1].plot(ss_true, label='true', color='orange')
            ax[2].set_title('Solar Noon')
            ax[2].plot(np.average(
             [self.sunrise_estimates, self.sunset_estimates], axis=0
            ), label='estimated', ls='--')
            ylims.append(ax[2].get_ylim())
            ax[2].plot(np.average(
                [self.sunrise_measurements, self.sunset_measurements], axis=0
            ), label='measured', marker='.', ls='none', alpha=0.3, color='green')
            if groundtruth is not None:
                ax[2].plot(np.average(
                    [sr_true, ss_true], axis=0
                ), label='true', color='orange')
            ax[3].set_title('Daylight Hours')
            ax[3].plot(self.sunset_estimates - self.sunrise_estimates,
                       label='estimated', ls='--')
            ylims.append(ax[3].get_ylim())
            ax[3].plot(self.sunset_measurements - self.sunrise_measurements,
                       label='measured', marker='.',
                       ls='none', alpha=0.3, color='green')
            if groundtruth is not None:
                ax[3].plot(ss_true - sr_true, label='true', color='orange')
            for i in range(4):
                ax[i].legend()
            if zoom_fit:
                for ax_it, ylim_it in zip(ax, ylims):
                    ax_it.set_ylim(ylim_it)
            plt.tight_layout()
            return fig
        else:
            return

    def run_optimizer(self, data, random_seed=None, plot=False,
                      figsize=(8, 6), groundtruth=None):
        if groundtruth is not None:
            sr_true = groundtruth[0]
            ss_true = groundtruth[1]
        else:
            sr_true = None
            ss_true = None
        ths = np.logspace(-5, -1, 51)
        ho_error = []
        full_error = []
        for th in ths:
            bool_msk = detect_sun(data, th)
            measured = rise_set_rough(bool_msk)
            sunrises = measured['sunrises']
            sunsets = measured['sunsets']
            np.random.seed(random_seed)
            use_set_sr = np.arange(len(sunrises))[~np.isnan(sunrises)]
            use_set_ss = np.arange(len(sunsets))[~np.isnan(sunsets)]
            if len(use_set_sr) / len(sunrises) > 0.6 and len(use_set_ss) / len(sunsets) > 0.6:
                run_ho_errors = []
                for run in range(5):
                    np.random.shuffle(use_set_sr)
                    np.random.shuffle(use_set_ss)
                    split_at_sr = int(len(use_set_sr) * .8)     # 80-20 train test split
                    split_at_ss = int(len(use_set_ss) * .8)
                    train_sr = use_set_sr[:split_at_sr]
                    train_ss = use_set_ss[:split_at_ss]
                    test_sr = use_set_sr[split_at_sr:]
                    test_ss = use_set_ss[split_at_ss:]
                    train_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
                    train_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
                    train_msk_sr[train_sr] = True
                    train_msk_ss[train_ss] = True
                    test_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
                    test_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
                    test_msk_sr[test_sr] = True
                    test_msk_ss[test_ss] = True
                    sr_smoothed = local_quantile_regression_with_seasonal(sunrises,
                                                                          train_msk_sr,
                                                                          tau=0.05,
                                                                          solver='MOSEK')
                    ss_smoothed = local_quantile_regression_with_seasonal(sunsets,
                                                                          train_msk_ss,
                                                                          tau=0.95,
                                                                          solver='MOSEK')
                    r1 = (sunrises - sr_smoothed)[test_msk_sr]
                    r2 = (sunsets - ss_smoothed)[test_msk_ss]
                    ho_resid = np.r_[r1, r2]
                    run_ho_errors.append(np.sqrt(np.mean(ho_resid ** 2)))
                ho_error.append(np.average(run_ho_errors))
                if groundtruth is not None:
                    full_fit = rise_set_smoothed(measured, sunrise_tau=0.05,
                                                 sunset_tau=0.95)
                    sr_full = full_fit['sunrises']
                    ss_full = full_fit['sunsets']
                    e1 = (sr_true - sr_full)
                    e2 = (ss_true - ss_full)
                    e_both = np.r_[e1, e2]
                    full_error.append(np.sqrt(np.mean(e_both ** 2)))
            else:
                ho_error.append(1e2)
                full_error.append(1e2)
        ho_error = np.array(ho_error)
        min_val = np.min(ho_error)
        slct_vals = ho_error < 1.2 * min_val
        selected_th = np.min(ths[slct_vals])
        bool_msk = detect_sun(data, selected_th)
        measured = rise_set_rough(bool_msk)
        smoothed = rise_set_smoothed(measured, sunrise_tau=.05,
                                     sunset_tau=.95)
        self.sunrise_estimates = smoothed['sunrises']
        self.sunset_estimates = smoothed['sunsets']
        self.sunrise_measurements = measured['sunrises']
        self.sunset_measurements = measured['sunsets']
        self.sunup_mask = bool_msk
        self.threshold = selected_th
        if groundtruth is not None:
            sr_residual = sr_true - self.sunrise_estimates
            ss_residual = ss_true - self.sunset_estimates
            total_rmse = np.sqrt(np.mean(np.r_[sr_residual, ss_residual] ** 2))
            self.total_rmse = total_rmse
        else:
            self.total_rmse = None

        if plot:
            fig = plt.figure(figsize=figsize)
            plt.plot(ths, ho_error, marker='.', color='blue', label='HO error')
            plt.yscale('log')
            plt.xscale('log')

            plt.plot(ths[slct_vals], ho_error[slct_vals], marker='.',
                     ls='none', color='red')
            plt.axvline(selected_th, color='blue', ls='--',
                        label='optimized parameter')
            if groundtruth is not None:
                best_th = ths[np.argmin(full_error)]
                plt.plot(ths, full_error, marker='.', color='orange',
                         label='true error')
                plt.axvline(best_th, color='orange', ls='--',
                            label='best parameter')
            plt.legend()
            return fig
        else:
            return


class SunriseSunset_v2():
    def __init__(self):
        self.sunrise_estimates = None
        self.sunset_estimates = None
        self.sunrise_measurements = None
        self.sunset_measurements = None
        self.sunup_mask = None
        self.threshold = None

    def run(self, data, random_seed=None):
        ths = np.logspace(-5, -1, 31)
        ho_error = []
        for th in ths:
            bool_msk = detect_sun(data, th)
            measured = rise_set_rough(bool_msk)
            sunrises = measured['sunrises']
            sunsets = measured['sunsets']
            # np.random.seed(random_seed)
            use_set_sr = np.arange(len(sunrises))[~np.isnan(sunrises)]
            use_set_ss = np.arange(len(sunsets))[~np.isnan(sunsets)]
            if len(use_set_sr) / len(sunrises) > 0.6 and len(use_set_ss) / len(sunsets) > 0.6:
                selected_th = th
                break
            else:
                selected_th = None
            #     np.random.shuffle(use_set_sr)
            #     np.random.shuffle(use_set_ss)
            #     split_at_sr = int(len(use_set_sr) * .8)     # 80-20 train test split
            #     split_at_ss = int(len(use_set_ss) * .8)
            #     train_sr = use_set_sr[:split_at_sr]
            #     train_ss = use_set_ss[:split_at_ss]
            #     test_sr = use_set_sr[split_at_sr:]
            #     test_ss = use_set_ss[split_at_ss:]
            #     train_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
            #     train_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
            #     train_msk_sr[train_sr] = True
            #     train_msk_ss[train_ss] = True
            #     test_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
            #     test_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
            #     test_msk_sr[test_sr] = True
            #     test_msk_ss[test_ss] = True
            #     sr_smoothed = local_quantile_regression_with_seasonal(sunrises,
            #                                                           train_msk_sr,
            #                                                           tau=0.05,
            #                                                           solver='MOSEK')
            #     ss_smoothed = local_quantile_regression_with_seasonal(sunsets,
            #                                                           train_msk_ss,
            #                                                           tau=0.95,
            #                                                           solver='MOSEK')
            #     r1 = (sunrises - sr_smoothed)[test_msk_sr]
            #     r2 = (sunsets - ss_smoothed)[test_msk_ss]
            #     ho_resid = np.r_[r1, r2]
            #     ho_error.append(np.sqrt(np.mean(ho_resid ** 2)))
            # else:
            #     ho_error.append(1e6)
            # selected_th = ths[np.argmin(ho_error)]
        bool_msk = detect_sun(data, selected_th)
        measured = rise_set_rough(bool_msk)
        smoothed = rise_set_smoothed(measured, sunrise_tau=.05,
                                     sunset_tau=.95)
        self.sunrise_estimates = smoothed['sunrises']
        self.sunset_estimates = smoothed['sunsets']
        self.sunrise_measurements = measured['sunrises']
        self.sunset_measurements = measured['sunsets']
        self.sunup_mask = bool_msk
        self.threshold = selected_th

class SunriseSunset_v1():
    def __init__(self):
        self.sunrise_estimates = None
        self.sunset_estimates = None
        self.sunrise_measurements = None
        self.sunset_measurements = None
        self.sunup_mask = None
        self.threshold = None

    def run(self, data, random_seed=None):
        ths = np.logspace(-5, -1, 31)
        ho_error = []
        for th in ths:
            bool_msk = detect_sun(data, th)
            measured = rise_set_rough(bool_msk)
            sunrises = measured['sunrises']
            sunsets = measured['sunsets']
            np.random.seed(random_seed)
            use_set_sr = np.arange(len(sunrises))[~np.isnan(sunrises)]
            use_set_ss = np.arange(len(sunsets))[~np.isnan(sunsets)]
            if len(use_set_sr) / len(sunrises) > 0.6 and len(use_set_ss) / len(sunsets) > 0.6:
                np.random.shuffle(use_set_sr)
                np.random.shuffle(use_set_ss)
                split_at_sr = int(len(use_set_sr) * .8)     # 80-20 train test split
                split_at_ss = int(len(use_set_ss) * .8)
                train_sr = use_set_sr[:split_at_sr]
                train_ss = use_set_ss[:split_at_ss]
                test_sr = use_set_sr[split_at_sr:]
                test_ss = use_set_ss[split_at_ss:]
                train_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
                train_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
                train_msk_sr[train_sr] = True
                train_msk_ss[train_ss] = True
                test_msk_sr = np.zeros_like(sunrises, dtype=np.bool)
                test_msk_ss = np.zeros_like(sunsets, dtype=np.bool)
                test_msk_sr[test_sr] = True
                test_msk_ss[test_ss] = True
                sr_smoothed = local_quantile_regression_with_seasonal(sunrises,
                                                                      train_msk_sr,
                                                                      tau=0.05,
                                                                      solver='MOSEK')
                ss_smoothed = local_quantile_regression_with_seasonal(sunsets,
                                                                      train_msk_ss,
                                                                      tau=0.95,
                                                                      solver='MOSEK')
                r1 = (sunrises - sr_smoothed)[test_msk_sr]
                r2 = (sunsets - ss_smoothed)[test_msk_ss]
                ho_resid = np.r_[r1, r2]
                ho_error.append(np.sqrt(np.mean(ho_resid ** 2)))
            else:
                ho_error.append(1e6)
        selected_th = ths[np.argmin(ho_error)]
        bool_msk = detect_sun(data, selected_th)
        measured = rise_set_rough(bool_msk)
        smoothed = rise_set_smoothed(measured, sunrise_tau=.05,
                                     sunset_tau=.95)
        self.sunrise_estimates = smoothed['sunrises']
        self.sunset_estimates = smoothed['sunsets']
        self.sunrise_measurements = measured['sunrises']
        self.sunset_measurements = measured['sunsets']
        self.sunup_mask = bool_msk
        self.threshold = selected_th