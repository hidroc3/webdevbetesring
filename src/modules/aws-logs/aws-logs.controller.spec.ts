import { Test, TestingModule } from '@nestjs/testing';
import { AwsLogsController } from './aws-logs.controller';

describe('AwsLogsController', () => {
  let controller: AwsLogsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AwsLogsController],
    }).compile();

    controller = module.get<AwsLogsController>(AwsLogsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
