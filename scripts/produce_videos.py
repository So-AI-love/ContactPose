import init_paths
from scripts.download_data import ContactPoseDownloader
import ffmpeg
import os
import shutil
import json
import itertools
from multiprocessing import Pool
import argparse
import dropbox
import time
import datetime

osp = os.path
intents = ('use', )
object_names = (
    'apple',
    'bowl',
    'cell_phone',
    'cup',
    'knife',
    'mug',
    'pan',
    'water_bottle',
    'wine_glass'
)
with open(osp.join('data', 'urls.json'), 'r') as f:
  urls = json.load(f)
urls = urls['images']
dropbox_app_key = os.environ.get('DROPBOX_APP_KEY')

def upload_dropbox(lfilename, dfilename):
  dbx = dropbox.Dropbox(dropbox_app_key)
  ddir, _ = osp.split(dfilename)
  ddir_exists = True
  try:
    dbx.files_get_metadata(ddir)
  except dropbox.exceptions.ApiError as err:
    ddir_exists = False
  if not ddir_exists:
    try:
      dbx.files_create_folder(ddir)
    except dropbox.exceptions.ApiError as err:
      print('*** API error', err)
      return False
  mtime = os.path.getmtime(lfilename)
  with open(lfilename, 'rb') as f:
    ldata = f.read()
  try:
    res = dbx.files_upload(
        ldata, dfilename,
        dropbox.files.WriteMode.overwrite,
        client_modified=datetime.datetime(*time.gmtime(mtime)[:6]),
        mute=True)
  except dropbox.exceptions.ApiError as err:
    print('*** API error', err)
    return False
  print('uploaded as', res.name.encode('utf8'))
  dbx.close()
  return True


def produce_worker(task):
  try:
    p_num, intent, object_name = task
    p_id = 'full{:d}_{:s}'.format(p_num, intent)
    dload_dir=osp.join('data', 'contactpose_data')
    data_dir = osp.join(dload_dir, p_id, object_name, 'images_full')

    # download
    downloader = ContactPoseDownloader()
    if osp.isdir(data_dir):
      shutil.rmtree(data_dir)
      print('Deleted {:s}'.format(data_dir))
    downloader.download_images(p_num, intent, dload_dir, include_objects=(object_name,))
    status = osp.isdir(data_dir)
    if not status:
      print('Could not download {:s} {:s}'.format(p_id, object_name))
      # check if the data actually exists
      if object_name in urls[p_id]:
        return status
      else:
        print('But that is OK because underlying data does not exist')
        return True
    
    # process
    for camera_position in ('left', 'right', 'middle'):
      camera_name = 'kinect2_{:s}'.format(camera_position)
      this_data_dir = osp.join(data_dir, camera_name)
      if not osp.isdir(this_data_dir):
        print('{:s} does not have {:s} camera'.format(this_data_dir, camera_position))
        continue
      output_filename = osp.join(this_data_dir, 'color.mp4')
      (
          ffmpeg
          .input(osp.join(this_data_dir, 'color', 'frame%03d.png'), framerate=30)
          .output(output_filename, pix_fmt='yuv420p', vcodec='libx264')
          .overwrite_output()
          .run()
      )
      print('{:s} written'.format(output_filename), flush=True)
      shutil.rmtree(osp.join(this_data_dir, 'color'))
      shutil.rmtree(osp.join(this_data_dir, 'depth'))

      # upload
      dropbox_path = osp.join('/', 'contactpose', 'videos_full', p_id, object_name,
          '{:s}_color.mp4'.format(camera_name))
      if not upload_dropbox(output_filename, dropbox_path):
        status = False
        break
    return status
  except Exception as e:
    print('Error somewhere in ', task)
    print(str(e))
    return False


def produce(p_nums, cleanup=False, parallel=True):
  if cleanup:
    print('#### Cleanup mode ####')
    filename = osp.join('status.json')
    with open(filename, 'r') as f:
      status = json.load(f)
    tasks = []
    for task,done in status.items():
      if done:
        continue
      task = task.split('_')
      p_num = int(task[0][4:])
      intent = task[1]
      object_name = '_'.join(task[2:])
      tasks.append((p_num, intent, object_name))
    print('Found {:d} cleanup items'.format(len(tasks)))
  else:
    tasks = list(itertools.product(p_nums, intents, object_names))
  
  if parallel:
    p = Pool(len(object_names))
    dones = p.map(produce_worker, tasks)
    p.close()
    p.join()
  else:
    dones = map(produce_worker, tasks)
  
  filename = osp.join('status.json')
  d = {}
  if osp.isfile(filename):
    with open(filename, 'r') as f:
      d = json.load(f)
  for task, done in zip(tasks, dones):
    d['full{:d}_{:s}_{:s}'.format(*task)] = done
  with open(filename, 'w') as f:
    json.dump(d, f, indent=4, separators=(', ', ': '))
  print('{:s} updated'.format(filename))


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-p', type=int, required=True)
  parser.add_argument('--cleanup', action='store_true')
  args = parser.parse_args()
  produce((args.p, ), cleanup=args.cleanup)
