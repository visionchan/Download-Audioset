import os
import shutil
from yt_dlp import YoutubeDL
import soundfile as sf
from moviepy.video.io.VideoFileClip import VideoFileClip
from moviepy.editor import AudioFileClip
from moviepy.config import change_settings
from tqdm import tqdm
from joblib import delayed, Parallel
import numpy as np
import tempfile
import json
from time import sleep
import logging
from datetime import datetime
import subprocess as sp
import ffmpy

from pathlib import Path

TMP_DIR = Path("E:/sedDatasets/AudioSet/tmp")
os.makedirs(TMP_DIR, exist_ok=True)

log_file = os.path.join(TMP_DIR, f'download_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def cleanup_temp_files(verbose=False):
    patterns = ["*TEMP_MPY*", "temp_*", "*_.mp4", "*.m4a", "*.wav"]
    for pattern in patterns:
        for temp_file in TMP_DIR.glob(pattern):
            try:
                os.remove(temp_file)
            except Exception as e:
                if verbose:
                    print(f"Failed to remove temp file {temp_file}: {e}")

def convert_audio_to_wav(input_path, output_path, start_time=None, end_time=None, sample_rate=16000):
    
    if not os.path.exists(input_path):
        print(f"[{input_path}] File not found")
        return False
    
    ff = ffmpy.FFmpeg(
        inputs={
            input_path: f'-ss {start_time} -t {end_time - start_time}'
            },
        outputs={
            output_path: f'-acodec pcm_s16le -ar {sample_rate} -ac 1 -y'
            },
        global_options={"-loglevel quiet"},
    )
    ff.run()
    
    # Remove temp audio
    if os.path.exists(input_path):
        os.remove(input_path)

    print(f"Successfully converted {input_path} to {output_path}")
    return True

def download_a_video_audio(
    faulty_files,
    data_dir,
    audio_id,
    url,
    labels,
    start_time=None,
    end_time=None,
    mode="video",
    timeout=1,
    verbose=True,
):
    download_status = False
    video_id = url.split("=")[-1]

    try:
        end_time_save = end_time
        all_files_exist = True
        for label in labels:
            if mode in ["only_audio", "both_separate"]:
                label_dir_audio = os.path.join(data_dir,"audio",label)
                os.makedirs(label_dir_audio, exist_ok=True)
            label_dir_video = os.path.join(data_dir,"video",label)
            os.makedirs(label_dir_video, exist_ok=True)

            if mode in ["video", "only_video", "both_separate"]:
                video_path = os.path.join(
                    label_dir_video,
                    f'video_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.mp4'
                )
                if not os.path.exists(video_path):
                    all_files_exist = False
                    break

            if mode in ["only_audio", "both_separate"]:
                audio_path = os.path.join(
                    label_dir_audio,
                    f'audio_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.wav'
                )
                if not os.path.exists(audio_path):
                    all_files_exist = False
                    break

        if all_files_exist:
            if verbose:
                logger.info(f"[{audio_id}] Files already exist for {url}, skipping...")
            return True

        existing_video = None
        for file in os.listdir(TMP_DIR):
            if video_id in file and file.endswith('.mp4'):
                existing_video = os.path.join(TMP_DIR, file)
                if verbose:
                    logger.info(f"[{audio_id}] Found existing video file: {existing_video}")
                break

        if not existing_video:
            if verbose:
                logger.info(f"[{audio_id}] Start downloading {url}")
            if mode in ["both_separate"]:
                video_opts = {
                    'quiet': not verbose,
                    'no_warnings': not verbose,
                    'proxy': 'http://127.0.0.1:7890',
                    'outtmpl': os.path.join(TMP_DIR, f'%(id)s.%(ext)s'),
                    'skip_unavailable_fragments': True,
                    'ignoreerrors': True,
                    'age_limit': None,
                    'format': 'bestvideo[ext=mp4]',
                    # 'cookiefile': './need_cookies.txt',
                }
                audio_opts = {
                    'quiet': not verbose,
                    'no_warnings': not verbose,
                    'proxy': 'http://127.0.0.1:7890',
                    'outtmpl': os.path.join(TMP_DIR, f'%(id)s.%(ext)s'),
                    'skip_unavailable_fragments': True,
                    'ignoreerrors': True,
                    'age_limit': None,
                    'format': 'bestaudio/best', 
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'm4a',
                    }],
                    # 'cookiefile': './need_cookies.txt',
                }
            elif mode in ["video"]:
                video_opts = audio_opts = {
                    'quiet': not verbose,
                    'no_warnings': not verbose,
                    'proxy': 'http://127.0.0.1:7890',
                    'outtmpl': os.path.join(TMP_DIR, f'%(id)s.%(ext)s'),
                    'skip_unavailable_fragments': True,
                    'ignoreerrors': True,
                    'age_limit': None,
                    'format': 'best[ext=mp4]',
                }
            with YoutubeDL(video_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info is None:
                    logger.error(f"[{audio_id}] Failed to download file for {url}")
                    raise Exception(f"Failed to download file for {url}")
                video_path = os.path.join(TMP_DIR, f'{video_id}.mp4')
                ydl.download([url])
                if not os.path.exists(video_path):
                    logger.error(f"[{audio_id}] Downloaded file not found at {video_path}")
                    raise Exception(f"Downloaded file not found at {video_path}")
                if verbose:
                    print(f"[{audio_id}] Successfully downloaded: {video_path}")
                    logger.info(f"[{audio_id}] Successfully downloaded: {video_path}")
            with YoutubeDL(audio_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info is None:
                    logger.error(f"[{audio_id}] Failed to download file for {url}")
                    raise Exception(f"Failed to download file for {url}")
                audio_path = os.path.join(TMP_DIR, f'{video_id}.m4a')
                ydl.download([url])
                if not os.path.exists(audio_path):
                    logger.error(f"[{audio_id}] Downloaded file not found at {audio_path}")
                    raise Exception(f"Downloaded file not found at {audio_path}")
                if verbose:
                    print(f"[{audio_id}] Successfully downloaded: {audio_path}")
                    logger.info(f"[{audio_id}] Successfully downloaded: {audio_path}")
            print(f"[{audio_id}] Successfully downloaded video and audio")
        print(f"[{audio_id}] video_path: {video_path}")
        print(f"[{audio_id}] audio_path: {audio_path}")

        if mode in ["video", "only_video", "both_separate"] and start_time != None:
            print(f"[{audio_id}] Start processing video")
            original_id = audio_id
            # safe_video_id = video_id.replace('-', '_') if video_id.startswith('-') else video_id
            video_path_tmp = os.path.join(TMP_DIR, f'{video_id}_processed.mp4')
            print(f"[{audio_id}] video_path_tmp: {video_path_tmp}")
            audio_path_tmp_wav = os.path.join(TMP_DIR, f'{video_id}_processed.wav')
            print(f"[{audio_id}] audio_path_tmp_wav: {audio_path_tmp_wav}")
            with VideoFileClip(video_path) as video:
                # get real video duration
                actual_duration = video.duration
                # check video duration
                if actual_duration <= end_time:
                    print(f"Video duration ({actual_duration}s) is shorter than requested end time ({end_time}s)")   
                    end_time = actual_duration
                    print("---------------------------end time is changed: ----------------------------------------", end_time)
                if start_time >= actual_duration:
                    raise Exception(f"Start time ({start_time}s) exceeds video duration ({actual_duration}s)")
                if end_time <= start_time:
                    raise Exception(f"End time ({end_time}s) must be greater than start time ({start_time}s)")
                new_video = video.subclip(start_time, end_time)
                convert_audio_to_wav(audio_path, 
                                     audio_path_tmp_wav, 
                                     start_time, 
                                     end_time)
                new_video.write_videofile(video_path_tmp, 
                                          audio_codec='aac', 
                                          logger=None)
                print("Successfully processed video to: ", video_path_tmp)
                print("Successfully processed audio to: ", audio_path_tmp_wav)
                if not os.path.exists(video_path_tmp) or os.path.getsize(video_path_tmp) == 0:
                    print(f"Processed video file not found at {video_path_tmp}")
                    raise Exception(f"Processed video file not found at {video_path_tmp}")
            if os.path.exists(video_path_tmp) and os.path.isfile(video_path_tmp) and os.path.getsize(video_path_tmp) > 0:
                os.remove(video_path)
                print("Successfully removed original video file: ", video_path)
        for label in labels:
            video_save_path = os.path.join(
                data_dir,
                "video",
                label,
                f"video_{original_id}_start_{int(start_time)}_end_{int(end_time_save)}.mp4"
            )
            audio_save_path = os.path.join(
                data_dir,
                "audio",
                label,
                f"audio_{original_id}_start_{int(start_time)}_end_{int(end_time_save)}.wav"
            )
            os.makedirs(os.path.dirname(video_save_path), exist_ok=True)
            os.makedirs(os.path.dirname(audio_save_path), exist_ok=True)
            if mode in ["only_audio", "both_separate"]:
                shutil.copy(audio_path_tmp_wav, audio_save_path)
                print(f"Successfully copied audio to: {audio_save_path}")
            if mode in ["both_separate", "video", "only_video"]:
                shutil.copy(video_path_tmp, video_save_path)
                print(f"Successfully copied video to: {video_save_path}")

        # make sure that the temp files are removed

        while os.path.isfile(video_path_tmp):
            os.remove(video_path_tmp)
        while os.path.isfile(audio_path_tmp_wav):
            os.remove(audio_path_tmp_wav)
        print("Successfully removed temporary video file: ", video_path_tmp)
        print("Successfully removed temporary audio file: ", audio_path_tmp_wav)

        download_status = True

    except Exception as e:
        if verbose:
            print(f"Error: {e}")
        faulty_files.append(f"{audio_id + 4} {start_time} {end_time_save} {labels} {url}")
        faulty_files.append(e)
        download_status = False

    return download_status

def parallel_download(data, args):
    steps = 2
    faulty_files = []
    progress_file = os.path.join(TMP_DIR, "download_progress.json")

    logger.info("Starting parallel download process")
    logger.info(f"Total files to process: {len(data.index)}")
    completed_ids = set()
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                completed_ids = set(json.load(f))
            print(f"Found {len(completed_ids)} completed downloads")
        except:
            completed_ids = set()

    if not os.path.isdir(TMP_DIR):
        os.makedirs(TMP_DIR, exist_ok=True)
    pending_indices = [i for i in range(len(data.index)) if str(data.index[i]) not in completed_ids]
    print(f"Remaining tasks: {len(pending_indices)}")
    batch_size = steps
    for i in range(0, len(pending_indices), batch_size):
        batch_indices = pending_indices[i:i + batch_size]
        try:
            batch_results = Parallel(n_jobs=steps)(
                delayed(download_a_video_audio)(
                    faulty_files,
                    args.destination_dir,
                    data.index[idx],
                    data.url[idx],
                    data.lables[idx],
                    data.start[idx],
                    data.end[idx],
                    args.mode,
                    args.verbose,
                )
                for idx in tqdm(batch_indices, desc="Downloading")
            )
            for idx, success in zip(batch_indices, batch_results):
                if success:
                    completed_ids.add(str(data.index[idx]))
                    data.download_status[idx] = True
                    with open(progress_file, 'w') as f:
                        json.dump(list(completed_ids), f)
                else:
                    faulty_files.append(
                        f"{data.index[idx]} {data.start[idx]} {data.end[idx]} {data.lables[idx]} {data.url[idx]}"
                    )

        except Exception as e:
            logger.error(f"Batch processing error: {str(e)}")
            continue

        print(f"Completed batch {i//batch_size + 1}/{(len(pending_indices) + batch_size - 1)//batch_size}, resting...")
        sleep(5)
    if faulty_files:
        error_filename = f"download_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        np.savetxt(error_filename, faulty_files, fmt="%s")
        logger.info(f"Saved error log to {error_filename}")

