import cv2
import numpy as np

# need refining
boundary = {
    'blue': np.array([[78, 43, 46], [155, 255, 255]]),
    'yellow': np.array([[11, 43, 46], [34, 255, 255]]),
    'green': np.array([[35, 43, 46], [77, 255, 255]]),
    'white': np.array([[0, 0, 221], [180, 30, 255]]),
    'black': np.array([[0, 0, 0], [180, 255, 46]])
}

color_plate = ['蓝牌', '黄牌', '绿牌', '白牌', '黑牌']
colors = ['blue', 'yellow', 'green', 'white', 'black']


def count_color_num(hsv_img, color):
    # may be more effecient
    mask = cv2.inRange(hsv_img, color[0], color[1])
    return np.count_nonzero(mask)


def judge_plate_color(img, threshold=0.5):
    # use HSV
    img = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    # pixel number
    total = img.shape[0] * img.shape[1]

    for i in range(5):
        num = count_color_num(img, boundary[colors[i]])
        if num / total > threshold:
            return color_plate[i]


if __name__ == "__main__":
    # function test
    img = cv2.imread('test_blue.jpg')
    print(judge_plate_color(img))
