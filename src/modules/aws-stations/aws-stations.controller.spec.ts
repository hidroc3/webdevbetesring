import { Test, TestingModule } from '@nestjs/testing';
import { AwsStationsController } from './aws-stations.controller';

describe('AwsStationsController', () => {
  let controller: AwsStationsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AwsStationsController],
    }).compile();

    controller = module.get<AwsStationsController>(AwsStationsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
